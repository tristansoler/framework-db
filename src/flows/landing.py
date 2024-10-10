from modules.config.core import ConfigSetup
from modules.config.versions.v1 import Enviroment

class ProcessingCoordinator:

    def __init__(self, payload: dict = None):

        self.config = ConfigSetup.initialize(payload=payload)
        print(self.config.environment)

        if self.config.environment == Enviroment.LOCAL:
            print("LOCAL")

    def process(self):
        """
        Método principal. Consiste en:
        -lectura
        -transformación del dato
        -escritura
        -creación de la partición
        """

    def _transformations(self, df):
        pass

    def _write_data(self, df):
        pass

if __name__ == '__main__':
    try:
        stb = ProcessingCoordinator()
        stb.process()
    except Exception as error:
        msg_error = 'Exception processing data Landing to Raw: %s' % str(error)
        logger.error(msg_error)
        raise error
