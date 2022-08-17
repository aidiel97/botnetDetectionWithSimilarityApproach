import os
from dotenv import load_dotenv

load_dotenv()

DEFAULT_COLUMN = str(os.getenv('DEFAULT_COLUMN')).split(' ') #get default column from string and split it
ATTACK_STAGE_LENGTH = int(os.getenv('ATTACK_STAGE_LENGTH'))
DEFAULT_MACHINE_LEARNING_TRAIN_PROPORTION = os.getenv('DEFAULT_MACHINE_LEARNING_TRAIN_PROPORTION')

MONGO_URL = os.getenv('MONGO_URL')
MONGO_DATABASE = os.getenv('MONGO_DATABASE')
MONGO_COLLECTION_DEFAULT = os.getenv('MONGO_COLLECTION_DEFAULT')
MONGO_DUMP_PATH = os.getenv('MONGO_DUMP_PATH')

DATASET_LOCATION = os.getenv('DATASET_LOCATION')
CTU_DIR = os.getenv('CTU_DIR')
NCC_DIR = os.getenv('NCC_DIR')
NCC2_DIR = os.getenv('NCC2_DIR')