from importlib import import_module


def main(dataflow: str, process: str, class_name: str):
    module_name = f'transformation.{dataflow}.{process}'
    common_module_name = f'transformation.common.{process}'

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
    _class.process()


if __name__ == '__main__':

    dataflow = 'my_dataflow'
    process = 'landing_to_raw'
    class_name = 'ProcessingCoordinator'

    main(dataflow, process, class_name)
