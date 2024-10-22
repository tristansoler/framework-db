from importlib import import_module
import sys

class Launcher:

    def main(self, dataflow: str, process: str):

        common_module_name = f'transformation.common.{process}'
        module_name = f'transformation.{dataflow}.{process}'

        if process == 'landing_to_raw':
            module_name = f'data_framework.flows.landing'

        self._execute(module_name=module_name, common_module_name=common_module_name)

    def get_parameters(self) -> dict:
        parameters = {}
        for i in range(1, len(sys.argv), 2):
            key = sys.argv[i].replace('--', '').replace('-', '_')
            value = sys.argv[i+1]
            parameters[key] = value
        
        return parameters

    def _execute(self, module_name: str, common_module_name: str):

        class_name = 'ProcessingCoordinator'

        try:
            print(f'Importing {module_name}')
            module = import_module(module_name)
        except ModuleNotFoundError:
            print(f'Module {module_name} not found. Importing {common_module_name}')
            module = import_module(common_module_name)

        try:
            print(f'Importing class {class_name}')
            _class = getattr(module, class_name)
        except AttributeError:
            print(f'Class {class_name} not found in {module.__name__}')

        print('Executing process')

        response = _class().process()
        if response.get('success') == False:
            exit(1)

if __name__ == '__main__':

    launcher = Launcher()
    
    parameters = launcher.get_parameters()

    dataflow = parameters.get('dataflow')
    process = parameters.get('process')
    
    launcher.main(dataflow, process)
