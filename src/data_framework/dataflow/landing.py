from data_framework.modules.dataflow.interface_dataflow import *
from data_framework.modules.storage.core_storage import Storage
from data_framework.modules.catalogue.core_catalogue import CoreCatalogue
from data_framework.modules.storage.interface_storage import Layer, Database
from data_framework.modules.validation.file_validator import FileValidator
import re
import hashlib
from datetime import datetime
from io import BytesIO
from pathlib import Path
from zipfile import ZipFile
import tarfile

class ProcessingCoordinator(DataFlowInterface):

    def __init__(self):
        super().__init__()

        self.storage = Storage()
        self.catalogue = CoreCatalogue()

    def process(self) -> dict:

        try:
            self.__payload_response.file_name = Path(self.config.parameters.source_file_path).name
            # Read file from S3
            file_contents = self.read_data()
            # Apply controls
            file_validator = FileValidator(file_contents)
            is_valid = file_validator.validate_file()
            if is_valid:
                # Obtain file date
                file_date = self.obtain_file_date()
                process_file = True

                # Compare with the previous file
                if self.incoming_file.compare_with_previous_file:
                    process_file = self.compare_with_previous_file(file_contents)
                    
                if process_file:
                    # Create partitions
                    partitions = self.create_partitions(file_date)
                    # Save file in raw table
                    self.write_data(file_contents, partitions)
                    self.__payload_response.next_stage = True
                    
                self.__payload_response.success = True
                self.__payload_response.file_date = file_date
        except Exception as e:
            self.logger.error(f'Error processing file {self.config.parameters.source_file_path}: {e}')

    def read_data(self) -> dict:
        response = self.storage.read(
            layer=Layer.LANDING,
            key_path=self.config.parameters.source_file_path
        )
        if not response.success:
            raise response.error
        s3_file_content = BytesIO(response.data)
        filename = Path(self.config.parameters.source_file_path).name
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

    def obtain_file_date(self) -> str:
        if self.incoming_file.csv_specs.date_located == 'filename':
            pattern = self.incoming_file.csv_specs.date_located_filename.regex
            match = re.search(pattern, self.config.parameters.source_file_path)
            year, month, day = match.groups()
            return f'{year}-{month}-{day}'
        elif self.incoming_file.csv_specs.date_located == 'column':
            # TODO: implementar
            return ''

    def compare_with_previous_file(self, file_contents: dict) -> bool:
        prefix = f'{config().parameters.dataflow}/processed'
        response = self.storage.list_files(Layer.LANDING, prefix)
        if response.success:
            incoming_filename = Path(self.config.parameters.source_file_path).name
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
                file_data['content'].seek(0)
                # TODO: revisar
                database_enum = [db for db in Database if db.value == self.output_file.database][0]
                self.storage.write(
                    layer=Layer.RAW,
                    database=database_enum,
                    table=self.output_file.table,
                    data=file_data['content'],
                    partitions=partitions,
                    filename=filename
                )


if __name__ == '__main__':
    stb = ProcessingCoordinator()
    stb.process()
