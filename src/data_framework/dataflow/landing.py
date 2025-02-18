from data_framework.modules.dataflow.interface_dataflow import (
    DataFlowInterface,
    ExecutionMode
)
from data_framework.modules.config.model.flows import DateLocated, LandingFileFormat
from data_framework.modules.storage.core_storage import Storage
from data_framework.modules.catalogue.core_catalogue import CoreCatalogue
from data_framework.modules.storage.interface_storage import Layer
from data_framework.modules.validation.integrations.file_validator import FileValidator
from data_framework.modules.exception.landing_exceptions import (
    FileProcessError,
    FileReadError,
    InvalidDateRegexError,
    InvalidRegexGroupError,
    InvalidFileError
)
import re
import hashlib
from typing import Tuple
from datetime import datetime
from io import BytesIO
from pathlib import Path
from zipfile import ZipFile
import tarfile
from pandas import read_xml, read_excel


class ProcessingCoordinator(DataFlowInterface):

    def __init__(self):
        super().__init__()

        self.storage = Storage()
        self.catalogue = CoreCatalogue()
        self.parameters = self.config.parameters

    def process(self):

        if self.parameters.execution_mode == ExecutionMode.DELTA:
            self.process_file()
        else:
            prefix = f'{self.config.project_id}/on_demand/'
            response = self.storage.list_files(layer=Layer.LANDING, prefix=prefix)
            for s3_key in response.result:
                current_file = Path(s3_key).name

                pattern = self.incoming_file.filename_pattern
                valid_filename = bool(re.match(pattern, current_file))

                if valid_filename:
                    try:
                        self.logger.info(f'[PROCESSING] {current_file}')
                        self.parameters.source_file_path = s3_key
                        self.process_file()
                        self.logger.info(f'[DONE] {current_file}')
                    except Exception:
                        self.logger.info(f'[ERROR] {current_file}')
                else:
                    self.logger.info(f'[SKIP] {current_file}')

    def process_file(self):
        try:
            self.payload_response.file_name = Path(self.parameters.source_file_path).name
            # Read file from S3
            file_contents = self.read_data()
            # Obtain file date
            file_date = self.obtain_file_date()
            self.payload_response.file_date = file_date
            # TODO: eliminar notificación
            self.notifications.send_notification(
                notification_name='file_arrival',
                arguments={
                    'dataflow': self.parameters.dataflow,
                    'process': self.parameters.process,
                    'file_name': Path(self.parameters.source_file_path).name,
                    'file_date': file_date
                }
            )
            # Apply controls
            file_validator = FileValidator(
                file_date=file_date,
                file_contents=file_contents,
                source_file_path=self.parameters.source_file_path
            )

            self.quality_controls.set_parent(file_validator)
            response = self.quality_controls.validate(
                layer=Layer.LANDING,
                table_config=self.config.processes.landing_to_raw.output_file
            )
            if response.overall_result:
                process_file = True
                # Compare with the previous file
                if self.incoming_file.compare_with_previous_file:
                    process_file = self.compare_with_previous_file(file_contents)
                if process_file:
                    # Create partitions
                    partitions = self.create_partitions(file_date)
                    # Save file in raw table
                    self.write_data(file_contents, partitions)
                    self.payload_response.next_stage = True
                self.payload_response.success = True
            else:
                raise InvalidFileError(file_path=self.parameters.source_file_path)
        except Exception:
            raise FileProcessError(file_path=self.parameters.source_file_path)

    def read_data(self) -> dict:
        try:
            response = self.storage.read(
                layer=Layer.LANDING,
                key_path=self.parameters.source_file_path
            )
            s3_file_content = BytesIO(response.data)
            filename = Path(self.parameters.source_file_path).name
            file_contents = {
                filename: {
                    'content': s3_file_content,
                    'validate': True
                }
            }
            if self.incoming_file.zipped == 'zip':
                file_contents[filename]['validate'] = False
                with ZipFile(s3_file_content, 'r') as z:
                    for filename in z.namelist():
                        with z.open(filename) as f:
                            file_contents[filename] = {
                                'content': BytesIO(f.read()),
                                'validate': True
                            }
            elif self.incoming_file.zipped == 'tar':
                file_contents[filename]['validate'] = False
                with tarfile.open(fileobj=s3_file_content, mode='r') as t:
                    for filename in t.getnames():
                        content = t.extractfile(filename).read()
                        file_contents[filename] = {
                            'content': BytesIO(content),
                            'validate': True
                        }
            return file_contents
        except Exception:
            raise FileReadError(file_path=self.parameters.source_file_path)

    def obtain_file_date(self) -> str:
        specifications = self.incoming_file.get_specifications()
        if specifications.date_located == DateLocated.FILENAME:
            filename = Path(self.parameters.source_file_path).name
            pattern = specifications.date_located_filename.regex
            match = re.search(pattern, filename)
            if not match:
                raise InvalidDateRegexError(filename=filename, pattern=pattern)
            elif match.groupdict():
                # Custom year-month-day order
                try:
                    year = match.group('year')
                    month = match.group('month')
                    day = match.group('day')
                except IndexError:
                    raise InvalidRegexGroupError(pattern=pattern)
            else:
                # Default year-month-day order
                year, month, day = match.groups()
            return f'{year}-{month}-{day}'
        elif specifications.date_located == DateLocated.COLUMN:
            # TODO: implementar
            raise NotImplementedError('Feature date_located = column is not implemented yet')

    def compare_with_previous_file(self, file_contents: dict) -> bool:
        prefix = f'{self.config.project_id}/processed'
        response = self.storage.list_files(Layer.LANDING, prefix)
        if response.success:
            incoming_filename = Path(self.parameters.source_file_path).name
            last_file_key = self.get_last_processed_file_key(incoming_filename, response.result)
            if last_file_key:
                self.logger.info('Comparing with last processed file')
                incoming_file_content = file_contents[incoming_filename]['content']
                last_file_content = BytesIO(
                    self.storage.read(
                        layer=Layer.LANDING,
                        key_path=last_file_key
                    ).data
                )
                incoming_file_hash = self.get_file_hash(incoming_file_content)
                last_file_hash = self.get_file_hash(last_file_content)
                if incoming_file_hash == last_file_hash:
                    self.logger.info('Incoming file and last processed file are the same')
                    return False
        return True

    def get_last_processed_file_key(self, incoming_filename: str, file_keys: str) -> str:
        date_pattern = r'insert_date=(\d{4}-\d{2}-\d{2})/insert_time=(\d{2}:\d{2}:\d{2})'
        matching_files = []
        for file_key in file_keys:
            if file_key.endswith(incoming_filename):
                match = re.search(date_pattern, file_key)
                if match:
                    insert_date = match.group(1)
                    insert_time = match.group(2)
                    insert_datetime = datetime.strptime(
                        f'{insert_date} {insert_time}',
                        '%Y-%m-%d %H:%M:%S'
                    )
                    matching_files.append((insert_datetime, file_key))
        if len(matching_files) > 0:
            matching_files.sort(reverse=True, key=lambda x: x[0])
            return matching_files[0][1]
        else:
            return ''

    def get_file_hash(self, file_content: BytesIO, chunk_size: int = 8000):
        file_content.seek(0)
        hasher = hashlib.md5()
        while chunk := file_content.read(chunk_size):
            hasher.update(chunk)
        hash_code = hasher.hexdigest()
        return hash_code

    def create_partitions(self, file_date: str) -> dict:
        partitions = {}

        partition_field = self.output_file.partition_field
        response = self.catalogue.create_partition(
            self.output_file.database_relation,
            self.output_file.table,
            partition_field,
            file_date
        )
        if response.success:
            partitions[partition_field] = file_date
        return partitions

    def write_data(self, file_contents: dict, partitions: dict) -> None:
        for filename, file_data in file_contents.items():
            if file_data['validate']:
                filename, file_content = self.convert_file_to_parquet(filename, file_data['content'])
                self.storage.write(
                    layer=Layer.RAW,
                    database=self.output_file.database,
                    table=self.output_file.table,
                    data=file_content,
                    partitions=partitions,
                    filename=filename
                )

    def convert_file_to_parquet(self, filename: str, file_content: BytesIO) -> Tuple[str, BytesIO]:
        file_content.seek(0)
        parquet_filename = re.sub(r'\.\w+$', '.parquet', filename)
        if self.incoming_file.file_format == LandingFileFormat.XML:
            self.logger.info(f'Converting XML file {filename} to parquet')
            df = read_xml(
                file_content,
                encoding=self.incoming_file.xml_specs.encoding,
                xpath=self.incoming_file.xml_specs.xpath,
                parser='etree',
                dtype=str
            )
            parquet_file_content = BytesIO()
            # TODO: parquet options
            df.to_parquet(parquet_file_content, index=False)
            parquet_file_content.seek(0)
            return parquet_filename, parquet_file_content
        elif self.incoming_file.file_format == LandingFileFormat.EXCEL:
            self.logger.info(f'Converting Excel file {filename} to parquet')
            # TODO: more excel parameters
            df = read_excel(file_content, dtype=str)
            parquet_file_content = BytesIO()
            # TODO: parquet options
            df.to_parquet(parquet_file_content, index=False)
            parquet_file_content.seek(0)
            return parquet_filename, parquet_file_content
        else:
            return filename, file_content


if __name__ == '__main__':
    stb = ProcessingCoordinator()
    stb.process()
