import csv
import os
import shutil
import sys

import jsonlines


def group_and_aggregate(filename, group_field, aggregate_field,
                        aggregate_function):
    # Создаем словарь для группировки и агрегации
    groups = {}

    # Открываем CSV-файл для чтения
    with open(filename, 'r') as file:
        reader = csv.reader(file)

        # Читаем каждую строку в файле
        for row in reader:
            if len(row) < group_field or len(row) < aggregate_field:
                continue
            # Получаем значение поля группировки
            group_value = int(row[group_field - 1])

            # Получаем значение поля агрегации
            aggregate_value = int(row[aggregate_field - 1])

            # Проверяем, существует ли уже группа в словаре
            if group_value in groups:
                # Добавляем значение агрегации к существующей группе
                groups[group_value].append(aggregate_value)
            else:
                # Создаем новую группу и добавляем значение агрегации
                groups[group_value] = [aggregate_value]

    # Производим агрегацию в соответствии с указанной функцией
    if aggregate_function == 'count':
        aggregation_per_file = {group: len(values) for group, values in
                                groups.items()}
    elif aggregate_function == 'max':
        aggregation_per_file = {group: max(values) for group, values in
                                groups.items()}
    elif aggregate_function == 'min':
        aggregation_per_file = {group: min(values) for group, values in
                                groups.items()}
    elif aggregate_function == 'sum':
        aggregation_per_file = {group: sum(values) for group, values in
                                groups.items()}
    else:
        print('Неподдерживаемая функция агрегации')
        return

    # Читаем построчно данные из общего файла с агрегацией информации (data.json)
    # Если за текущую итерацию агрегации мы можем что-то изменить в data.json,
    # то измененную информацию записываем в temp_data.json
    # Если никак не можем изменить старую информацию – дублируем ее в temp_data.json
    # В памяти всегда храним только 1 строку
    with jsonlines.open('data.json', 'r') as data_file:
        with jsonlines.open('temp_data.json', 'w') as temp_data_file:
            for line in data_file:
                is_line_aggregated = False
                for group, value in aggregation_per_file.items():
                    if group == line['group']:
                        if aggregate_function == 'sum':
                            line['value'] += value
                        elif aggregate_function == 'count':
                            line['value'] += 1
                        elif aggregate_function == 'min':
                            if line['value'] > value:
                                line['value'] = value
                        elif aggregate_function == 'max':
                            if line['value'] < value:
                                line['value'] = value

                        temp_data_file.write(line)
                        is_line_aggregated = True
                        # если зашли сюда –удаляем информацию,
                        # что привнес новый файл,
                        # т. к. уже записали в temp_data.json
                        del aggregation_per_file[group]
                        break

                # Если в текущей итерации агрегации нет информации
                # о рассматриваемой строчке,
                # просто дублируем ее во временный файл
                if not is_line_aggregated:
                    temp_data_file.write(line)

    # Записываем обновленную информацию на диск во временный файл temp_data
    with jsonlines.open('temp_data.json', 'a') as temp_data_file:
        for group, value in aggregation_per_file.items():
            temp_data_file.write({'group': group, 'value': value})

    # Копируем содержимое агрегации в основной файл data.json
    shutil.copyfile('temp_data.json', 'data.json')
    os.remove('temp_data.json')

    print(
        f'После обработки файла {filename} '
        f'итоговый результат агрегации обновлен в файле data.json обновлен'
    )


def split_file(filename, chunk_size):
    # Создаем директорию для хранения разбитых файлов
    output_dir = 'split_files'
    try:
        os.makedirs(output_dir)
    except FileExistsError:
        shutil.rmtree(output_dir)
        os.makedirs(output_dir)

    # Открываем исходный файл для чтения
    with open(filename, 'r') as file:
        reader = csv.reader(file)

        # Создаем переменные для отслеживания текущего размера и текущего файла
        current_size = 0
        current_file = None

        # Читаем каждую строку в файле
        for row in reader:
            # Проверяем, нужно ли создать новый файл
            if current_file is None or current_size >= chunk_size:
                # Закрываем предыдущий файл, если он был открыт
                if current_file is not None:
                    current_file.close()

                # Создаем новый файл
                file_number = len(os.listdir(output_dir)) + 1
                output_filename = os.path.join(output_dir,
                                               f'chunk_{file_number}.csv')
                current_file = open(output_filename, 'w')
                current_size = 0

            # Записываем строку в текущий файл
            current_file.write(','.join(row) + '\n')
            current_size += sys.getsizeof(row)

        # Закрываем последний файл
        if current_file is not None:
            current_file.close()

    print(
        f'Файл успешно разбит на более мелкие части в директории {output_dir}'
    )


if __name__ == "__main__":
    # Получаем аргументы командной строки
    filename = sys.argv[1]
    group_field = int(sys.argv[2])
    aggregate_field = int(sys.argv[3])
    aggregate_function = sys.argv[4]

    # Разбиваем исходный файл на более мелкие части
    split_file(filename, 3 * 1024 * 1024 * 1024)

    # Применяем агрегацию к каждой части файла
    split_files_dir = 'split_files'

    # Проверяем, существует ли файл data.json и удаляем его если существует
    # (чтобы для каждого запуска иметь новый data.json)
    if os.path.exists('data.json'):
        os.remove('data.json')

    open('data.json', 'w').close()

    for file in os.listdir(split_files_dir):
        file_path = os.path.join(split_files_dir, file)
        group_and_aggregate(file_path, group_field, aggregate_field,
                            aggregate_function)
