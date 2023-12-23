from typing import Generator
import csv
import copy
import datetime


def time_str_to_timedelta(time_str: str) -> datetime.timedelta:
    hours, minutes, sec = map(int, time_str.split(":"))
    return datetime.timedelta(hours=hours, minutes=minutes, seconds=sec)


def extended_lines_generator(reader: csv.DictReader) -> Generator:
    for line in reader:
        flight_start = time_str_to_timedelta(line.pop('break_flight_start'))
        flight_end = time_str_to_timedelta(line.pop('break_flight_end'))
        program_end = time_str_to_timedelta(line.pop('programme_flight_end'))
        programme_start = time_str_to_timedelta(line.pop('programme_flight_start'))

        date = datetime.datetime.strptime(line['date'], '%Y-%m-%d')
        real_date = copy.copy(date)
        if flight_start >= datetime.timedelta(hours=24):
            real_date += datetime.timedelta(days=1)
            flight_start -= datetime.timedelta(days=1)
            flight_end -= datetime.timedelta(days=1)

        real_program_start_date = copy.copy(date)
        if programme_start >= datetime.timedelta(hours=24):
            programme_start -= datetime.timedelta(days=1)
            program_end -= datetime.timedelta(days=1)
            real_program_start_date += datetime.timedelta(days=1)

        program_duration = program_end - programme_start

        duration = flight_end - flight_start

        line["program_duration"] = program_duration
        line["real_program_start"] = programme_start
        line["real_program_start_date"] = real_program_start_date.strftime('%Y-%m-%d')

        line["duration"] = duration
        line["real_flight_start"] = flight_start
        line["real_date"] = real_date.strftime('%Y-%m-%d')
        yield line


if __name__ == '__main__':
    # with open("../data/train_fixed_dates.csv", "w", newline='') as fixed_file:
    #     with open("../data/train.csv") as file:
    #         reader = csv.DictReader(file, delimiter=',',)
    #         fild_names = [
    #             name for name in reader.fieldnames
    #             if name not in (
    #                 'break_flight_start',
    #                 'break_flight_end',
    #                 'programme_flight_end',
    #                 'programme_flight_start',
    #             )
    #         ]
    #         fild_names.extend((
    #             "duration",
    #             "real_flight_start",
    #             "real_date",
    #             "program_duration",
    #             "real_program_start_date",
    #             "real_program_flight_start",
    #             "real_program_start"
    #         ))
    #         writer = csv.DictWriter(fixed_file, fieldnames=fild_names, delimiter=',')
    #         writer.writeheader()
    #         writer.writerows(extended_lines_generator(reader))

    with open("../data/test_fixed_dates.csv", "w", newline='') as fixed_file:
        with open("../data/test_data.csv") as file:
            reader = csv.DictReader(file, delimiter=',',)
            fild_names = [
                name for name in reader.fieldnames
                if name not in (
                    'break_flight_start',
                    'break_flight_end',
                    'programme_flight_end',
                    'programme_flight_start',
                )
            ]
            fild_names.extend((
                "duration",
                "real_flight_start",
                "real_date",
                "program_duration",
                "real_program_start_date",
                "real_program_flight_start",
                "real_program_start"
            ))
            writer = csv.DictWriter(fixed_file, fieldnames=fild_names, delimiter=',')
            writer.writeheader()
            writer.writerows(extended_lines_generator(reader))
