import argparse
import logging
import pickle
from collections import deque
from datetime import datetime
from time import sleep
import multiprocessing as mp

from faker import Faker

parser = argparse.ArgumentParser()
parser.add_argument('--ndata', type=int, default=10)
args = parser.parse_args()


N = args.ndata
AGE_BOTTOM = 30
AGE_TOP = 40

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s, %(levelname)s, %(message)s'
)


class DataGenerator:
    '''Генерация данных.'''
    def __init__(self):
        self.fake = Faker()
        self.dq_start = deque(maxlen=N)
        self.dq = deque(maxlen=N)

    def generate_a_profile(self):
        '''Генерирует один профайл.'''
        data = self.fake.profile()
        birthdate = data['birthdate']
        current_date = datetime.now()
        age = (current_date.year - birthdate.year - ((
                current_date.month,
                current_date.day
                ) < (
                birthdate.month,
                birthdate.day
            ))
        )
        data['age'] = age
        return data

    def generate_start(self):
        '''Генерирует начальный набор данных.'''
        for i in range(N):
            data = self.generate_a_profile()
            self.dq_start.append(data)
        return self.dq_start

    def generate(self, sleep_time=0.5):
        '''Генерирует данные каждые sleep_time секунд.'''
        while True:
            data = self.generate_a_profile()
            self.dq.append(data)
            sleep(sleep_time)


class DataProcessor:
    '''Класс валидации данных.'''
    def __init__(self, dq):
        self.dq = dq

    def process(self):
        with open('data.dat', 'ab') as f:
            for data in self.dq:
                if AGE_BOTTOM <= data['age'] <= AGE_TOP:
                    pickle.dump(data, f)


class DataSender:
    '''Класс получения данных из файла и отправки их на сервер.'''
    def __init__(self):
        self.data = []

    def get_data(self):
        '''Получает данные из файла и чистит файл.'''
        with (open("data.dat", "r+b")) as f:
            while True:
                try:
                    self.data.append(pickle.load(f))
                except EOFError:
                    f.truncate(0)
                    break

        return self.data

    def send(self, data):
        '''
        Имитирует отправку данных на сервер,
        логгирует это в терминал
        и вывод информацию о пользователе.
        '''
        logging.info('Данные отправлены на сервер')
        for profile in data:
            try:
                print(
                    'name -', profile['name'],
                    'sex -', profile['sex'],
                    'job -', profile['job'],
                    'age -', profile['age']
                    )
            except Exception as e:
                print(e)


def main():
    generator = DataGenerator()
    dq_start = generator.generate_start()

    # processor = DataProcessor(dq_start)
    # processor.process()

    # sender = DataSender()
    # data = sender.get_data()
    # sender.send(data)

    # # generator.generate()
    # # processor = DataProcessor(generator.dq)
    # # processor.process()

    # generator_process = mp.Process(target=generator.generate)
    processor_process = mp.Process(target=DataProcessor(dq_start).process())
    sender_process = mp.Process(
        target=DataSender().send,
        args=(DataSender().get_data(),)
    )

    # generator_process.start()
    processor_process.start()
    sender_process.start()

    # generator_process.join()
    processor_process.join()
    sender_process.join()

if __name__ == '__main__':
    main()
