import logging

import catboost
import pandas as pd
from sklearn.model_selection import train_test_split
from sqlalchemy import Boolean, Column, Float, Integer, text
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.sql import insert

from lib import AbstractTrainFlow, PreparedResult, mape


class CatboostTrainFlow(AbstractTrainFlow):
    model_name = 'model_008'

    def prepare_features(
        self,
        limit: int | None = None,
        for_test: bool | None = False,
        table_name: str = "train",
        sql_features_table_name: str = "train_003_features",  # test_data_003_features
        program_id_filter: int | None = None,
        program_genre_id_filter: int | None = None,
    ) -> PreparedResult:
        for_test_join = f"""
            join last_14_days_sampling 
                on {table_name}.break_flight_id = last_14_days_sampling.id     
                and for_test = {for_test}  
        """ if for_test is not None else ""
        program_id_filter = f" AND programmes.id = {program_id_filter}" if isinstance(program_id_filter, int) else ''
        program_genre_id_filter = f" AND programme_genres.id = {program_genre_id_filter}" \
            if isinstance(program_genre_id_filter, int) else ''
        select_query = f""" --sql
            SELECT 
                {table_name}.*,
                {sql_features_table_name}.* ,
                programme_categories.avg_int_target as pc_avg_int_target,
                programme_categories.effir_rate as pc_effir_rate,
                programme_categories.blocks_per_program as pc_blocks_per_program,
                programmes.avg_int_target as p_avg_int_target,
                programmes.effir_rate as p_effir_rate,
                programmes.blocks_per_program as p_blocks_per_program,
                programme_genres.avg_int_target as genre_avg_int_target,
                programme_genres.effir_rate as genre_effir_rate,
                programme_genres.blocks_per_program as genre_blocks_per_program,
                calendar.extra_holiday, 
                calendar.holiday, 
                calendar.week_day_3,
                time_weight.time_group,
                time_weight.weight as time_weight,
                time_weight_for_week_day.weight as week_day_time_weight,
                time_weight_for_week_day_by5_min.weight as week_day_time_weight_by5_min,
                time_weight_for_week_day_by150_sec.weight as week_day_time_weight_by150_sec,
                time_weight_for_week_day_by30_sec.weight as week_day_time_weight_by30_sec,
                time_weight_for_week_day_by300_sec_last_4_week.weight as last_4_week_week_day_300_sec_weight,
                time_weight_for_week_day_by300_sec_last_4_week.time_group as week_day_300_sec_group,
                time_weight_for_bd_week_day_by30_sec.weight as week_day_bd_time_weight_by30_sec,
                moscow_wather.*
            FROM {table_name}
                {for_test_join}
                join {sql_features_table_name} on {sql_features_table_name}.id = {table_name}.break_flight_id
                join programmes 
                    on {table_name}.programme_id = programmes.id {program_id_filter}
                join programme_categories 
                    on programme_categories.id = {table_name}.programme_category_id 
                join programme_genres 
                    on programme_genres.id = {table_name}.programme_genre_id {program_genre_id_filter}
                left join calendar on {table_name}.date = calendar.date
                left join time_weight on (EXTRACT(epoch FROM real_flight_start) / 600)::int = time_weight.time_group
                left join time_weight_for_week_day on 
                (
                    (EXTRACT(epoch FROM real_flight_start) / 600)::int = time_weight_for_week_day.time_group
                        AND
                    extract(dow from real_date) + 1 = time_weight_for_week_day.week_day_2
                )
                left join time_weight_for_week_day_by5_min on 
                (
                    (EXTRACT(epoch FROM real_flight_start) / 300)::int = time_weight_for_week_day_by5_min.time_group
                        AND
                    extract(dow from real_date) + 1 = time_weight_for_week_day_by5_min.week_day_2
                )
                left join time_weight_for_week_day_by150_sec on 
                (
                    (EXTRACT(epoch FROM real_flight_start) / 150)::int = time_weight_for_week_day_by150_sec.time_group
                        AND
                    extract(dow from real_date) + 1 = time_weight_for_week_day_by150_sec.week_day_2
                )
                left join time_weight_for_week_day_by30_sec on 
                (
                    (EXTRACT(epoch FROM real_flight_start) / 30)::int = time_weight_for_week_day_by30_sec.time_group
                        AND
                    extract(dow from real_date) + 1 = time_weight_for_week_day_by30_sec.week_day_2
                )
                left join time_weight_for_week_day_by300_sec_last_4_week on (
                    (EXTRACT(epoch FROM real_flight_start) / 300)::int 
                        = time_weight_for_week_day_by300_sec_last_4_week.time_group
                        AND
                    extract(dow from real_date) + 1 = time_weight_for_week_day_by300_sec_last_4_week.week_day_2
                )
                left join time_weight_for_bd_week_day_by30_sec on 
                (
                    (EXTRACT(epoch FROM real_flight_start) / 30)::int = time_weight_for_bd_week_day_by30_sec.time_group
                        AND
                    extract(dow from real_date) + 1 = time_weight_for_bd_week_day_by30_sec.week_day_2
                        AND
                    time_weight_for_bd_week_day_by30_sec.break_distribution_id = {table_name}.break_distribution_id
                )
                left join moscow_wather  
                    on {table_name}.date = moscow_wather.date 
                        and (extract(epoch from {table_name}.real_flight_start) / 60 / 60 / 3)::int =  hour                
        """
        if limit is not None and isinstance(limit, int):
            select_query += f" LIMIT {limit}"
        train_data = pd.read_sql(select_query, self.db_engine)
        logging.info('Data select done')

        target = ['non_zero_target']  # ['int_target']  # ['non_zero_target']  # ['int_target']
        exclude_but_keep = ['id', 'break_flight_id']
        num_features = [
            'real_flight_start_ts',
            'real_program_start_ts',
            'duration_ts',
            'program_duration_ts',
            'week_day',        # eaten by real_week_day_2
            'real_week_day',   # eaten by real_week_day_2
            'week_day_2',      # eaten by real_week_day_2
            'real_week_day_2',
            'week_day_3', # to week ??
            'programme_id', # to week ??
            'programme_category_id', # to week ??
            'programme_genre_id',  # to week
            'break_distribution_id',
            'break_content_id',  # to week

            'p_avg_int_target',
            'p_effir_rate',
            'p_blocks_per_program',

            'pc_avg_int_target',
            'pc_effir_rate',
            'pc_blocks_per_program', # to week ??

            # 'genre_avg_int_target', # to week ??
            # 'genre_effir_rate', # to week ??
            'genre_blocks_per_program', # to week ??

            'time_weight',
            'time_group',
            'week_day_time_weight',
            'week_day_time_weight_by5_min',
            'week_day_time_weight_by150_sec',
            'week_day_time_weight_by30_sec',

            'last_4_week_week_day_300_sec_weight',
            'week_day_300_sec_group',

            'week_day_bd_time_weight_by30_sec',

            't',
            'po',
            'p',
            'pa',
            'u',

            # have nulls
            # 'tn',
            # 'tx',
            # 'vv',
            # 'td',
            #'tr',

        ]
        bool_features = [
            'night_program',
            'extra_holiday',
            # 'holiday',
        ]
        text_features = [  # enums
            # 'programme',              # eaten out by programme_id
            # 'programme_category',     # eaten out by programme_category_id
            # 'programme_genre',        # to week
            # 'break_distribution',     # eaten out by break_distribution_id
            # 'break_content',          # to week


            # 'dd',
            # 'ff',
            # 'ff10',
            # 'ff3',
            # 'n',
            # 'ww',
            # 'w1',
            # 'w2',
            # 'cl',
            # 'nh',
            # 'h',
            # 'cm',
            # 'ch',
            # 'rrr',
            # 'e',
            # 'tg',
            # 'ee',
            # 'sss',
        ]
        used = target + num_features + bool_features + exclude_but_keep + text_features
        predictors = num_features + bool_features + text_features
        prepared_data = train_data.drop(columns=[col for col in train_data.columns if col not in used])
        logging.info('Data prepared')

        return PreparedResult(
            data=prepared_data,
            target_column=target,
            features_columns=predictors,
            text_features=text_features,
        )

    def learn(self, prepared_data: PreparedResult, test_prepared_data: PreparedResult | None = None):
        if test_prepared_data is None:
            X_train, X_test, y_train, y_test = train_test_split(
                prepared_data.features_frame,
                prepared_data.target_frame,
                test_size=0.2,
                random_state=119,
            )
        else:
            X_train, y_train = prepared_data.features_frame, prepared_data.target_frame
            X_test, y_test = test_prepared_data.features_frame, test_prepared_data.target_frame

        if len(y_test) == 0:
            X_train, X_test, y_train, y_test = train_test_split(
                prepared_data.features_frame,
                prepared_data.target_frame,
                test_size=0.2,
                random_state=119,
            )

        # Prepare model

        model = catboost.CatBoostRegressor(
            iterations=300,
            verbose=True,
            cat_features=prepared_data.text_features,
            loss_function='MAPE', # loss_function='LogLinQuantile',
        )

        # Fit model
        model.fit(X_train, y_train, eval_set=(X_test, y_test), verbose_eval=True)
        self.model = model
        pred = model.predict(X_test)
        learn_pred = model.predict(X_train[:25000])
        if test_prepared_data is not None:
            print("validate on last weeks")
        print(f"MAPE score: {mape(y_test, pred)}")
        # print(f"MAPE on learn score: {mape(y_train[:25000], learn_pred)}")
        print(model.get_feature_importance(prettified=True))

    def save_model(self):
        assert self.model is not None
        self.model.save_model(self.model_name)

    def load_model(self):
        from_file_model = catboost.CatBoostRegressor()
        from_file_model.load_model(self.model_name)
        self.model = from_file_model

    def apply_model_in_db(self, prepared_data: PreparedResult, table_name: str):
        assert self.model is not None
        predicts = self.model.predict(prepared_data.features_frame)
        logging.info('predicts calculated')
        ids = prepared_data.data[['break_flight_id']]

        Session = sessionmaker(bind=self.db_engine)
        with Session() as session:
            session.execute(text(f"DROP TABLE if exists {table_name};"))
            session.execute(text(
                f"""
                    CREATE TABLE {table_name} (
                        id bigint primary key,
                        score float
                    );
                """
            ))
            logging.info('result table created')

            Base = declarative_base()

            class PredictTable(Base):
                __tablename__ = table_name
                id = Column(Integer, primary_key=True)
                predict = Column(Boolean)
                score = Column(Float)

            id_with_predict_and_score = list(zip(ids['break_flight_id'], predicts))
            chunk_size = 10000
            for chunk in range(0, len(id_with_predict_and_score) // chunk_size + 1):
                if chunk * chunk_size < len(id_with_predict_and_score):
                    session.execute(
                        insert(PredictTable),
                        [
                            {'id': id_value, 'score': score * 0.01499250375}
                            for id_value, score
                            in id_with_predict_and_score[chunk * chunk_size:(chunk + 1) * chunk_size]
                        ],
                    )
                logging.info('saved chunk %r', chunk)
            session.commit()
            logging.info('saved to db finished')

    # def apply_model(self, prepared_data: PreparedResult):
    #     raise NotImplementedError


"""
249:	learn: 14.6883027	test: 21.3721749	best: 10.3983349 (68)	total: 1.81s	remaining: 0us

bestTest = 10.39833485
bestIteration = 68

Shrink model to first 69 iterations.
MAPE score: 0.9187870460784068
               Feature Id  Importances
0         real_week_day_2    23.797387
1              time_wight    19.740177
2   break_distribution_id    16.758248
3       pc_avg_int_target    14.890462
4           pc_effir_rate    12.447014
5        p_avg_int_target     6.183784
6    real_flight_start_ts     2.766813
7              time_group     1.380382
8   real_program_start_ts     0.889389
9            p_effir_rate     0.735066
10   p_blocks_per_program     0.184750
11          night_program     0.156774
12    program_duration_ts     0.061516
13            duration_ts     0.008237
"""

"""
98:	learn: 14.8231724	test: 18.4366920	best: 10.8227972 (68)	total: 5.07s	remaining: 51.2ms
99:	learn: 15.2245663	test: 18.7541340	best: 10.8227972 (68)	total: 5.14s	remaining: 0us

bestTest = 10.82279725
bestIteration = 68

Shrink model to first 69 iterations.
MAPE score: 0.9197242983022171
MAPE on learn score: 0.9111372893522895
                  Feature Id  Importances
0        week_day_time_wight    66.200852
1      break_distribution_id    25.040433
2                 time_wight     3.190546
3               p_effir_rate     1.626934



99:	learn: 14.9430449	test: 14.5722864	best: 10.7743703 (72)	total: 5.32s	remaining: 0us

bestTest = 10.77437032
bestIteration = 72

Shrink model to first 73 iterations.
MAPE score: 0.9120081229406203
MAPE on learn score: 0.9163759422074305
                  Feature Id  Importances
0        week_day_time_wight    74.231517
1      break_distribution_id    14.955431
2   genre_blocks_per_program     3.518114
3       genre_avg_int_target     1.997616
4      programme_category_id     1.746001

"""