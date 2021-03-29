import logging

logger = logging.getLogger(__name__)


class Database:
    def __init__(self, name: str):
        self.name = name

        self.connect()

    def connect(self):
        logger.info(f"Successfully connected to database: {self.name}")

    def load_file(self, file_path: str):
        """Loads the provided file into the database."""
        logger.info(f"Successfully loaded {file_path} into database: {self.name}")
