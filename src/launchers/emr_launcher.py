from importlib import import_module
import sys


class Launcher:

    def main(self, dataflow: str, process: str):

        common_module_name = f'transformation.dataflow.default.{process}'
        module_name = f'transformation.dataflow.{dataflow}.{process}'
        if process == 'landing_to_raw':
            module_name = 'data_framework.dataflow.landing'
        self._execute(module_name=module_name, default_module_name=common_module_name)

    def get_parameters(self) -> dict:
        parameters = {}
        for parameter_index in range(1, len(sys.argv), 2):
            key = sys.argv[parameter_index].replace('--', '').replace('-', '_')
            value = sys.argv[parameter_index+1]
            parameters[key] = value
        return parameters

    def _execute(self, module_name: str, default_module_name: str):

        class_name = 'ProcessingCoordinator'

        try:
            module = import_module(module_name)
        except ModuleNotFoundError:
            module = import_module(default_module_name)

        try:
            _class = getattr(module, class_name)
        except AttributeError:
            print(f'Class {class_name} not found in {module.__name__}')

        response = _class().process()
        if response is not None and response.get('success') is False:
            exit(1)


if __name__ == '__main__':

    launcher = Launcher()
    parameters = launcher.get_parameters()
    dataflow = parameters.get('dataflow')
    process = parameters.get('process')
    launcher.main(dataflow, process)
