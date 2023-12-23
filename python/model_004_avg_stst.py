import logging

import catboost
import pandas as pd
from sklearn.model_selection import train_test_split
from sqlalchemy import Boolean, Column, Float, Integer, text
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.sql import insert

from lib import AbstractTrainFlow, PreparedResult, mape


class CatboostTrainFlow(AbstractTrainFlow):
    model_name = 'model_003'

    def prepare_features(
        self,
        limit: int | None = None,
        for_test: bool = False,
    ) -> PreparedResult:
        select_query = f""" --sql
            SELECT 
                train.*,
                train_003_features.* ,
                programme_categories.avg_int_target as pc_avg_int_target,
                programme_categories.effir_rate as pc_effir_rate,
                programme_categories.blocks_per_program as pc_blocks_per_program,
                programmes.avg_int_target as p_avg_int_target,
                programmes.effir_rate as p_effir_rate,
                programmes.blocks_per_program as p_blocks_per_program,
                programme_genres.avg_int_target as genre_avg_int_target,
                programme_genres.effir_rate as genre_effir_rate,
                programme_genres.blocks_per_program as genre_blocks_per_program,
                calendar.*                
            FROM train
                join last_7_days_sampling 
                    on train.break_flight_id = last_7_days_sampling.id 
                    and for_test = {for_test}
                join train_003_features on train_003_features.id = train.break_flight_id
                join programmes on train.programme_id = programmes.id
                join programme_categories on programme_categories.id = train.programme_category_id 
                join programme_genres on programme_genres.id = train.programme_genre_id
                left join calendar on train.date = calendar.date
        """
        if limit is not None and isinstance(limit, int):
            select_query += f" LIMIT {limit}"
        train_data = pd.read_sql(select_query, self.db_engine)
        logging.info('Data select done')

        target = ['int_target']
        exclude_but_keep = ['id']
        num_features = [
            'real_flight_start_ts',
            'real_program_start_ts',
            'duration_ts',
            'program_duration_ts',
            # 'week_day',        # eaten by real_week_day_2
            # 'real_week_day',   # eaten by real_week_day_2
            # 'week_day_2',      # eaten by real_week_day_2
            'real_week_day_2',
            # 'week_day_3',
            # 'programme_id',
            # 'programme_category_id',
            # 'programme_genre_id',  # to week
            'break_distribution_id',
            # 'break_content_id',  # to week

            'p_avg_int_target',
            'p_effir_rate',
            'p_blocks_per_program',

            'pc_avg_int_target',
            'pc_effir_rate',
            # 'pc_blocks_per_program',

            # 'genre_avg_int_target',
            # 'genre_effir_rate',
            # 'genre_blocks_per_program',
        ]
        bool_features = [
            'night_program',
            # 'extra_holiday',
            # 'holiday',
        ]
        text_features = [  # enums
            # 'programme',              # eaten out by programme_id
            # 'programme_category',     # eaten out by programme_category_id
            # 'programme_genre',        # to week
            # 'break_distribution',     # eaten out by break_distribution_id
            # 'break_content',          # to week
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
                test_size=0.1,
                random_state=41,
            )
        else:
            X_train, y_train = prepared_data.features_frame, prepared_data.target_frame
            X_test, y_test = test_prepared_data.features_frame, test_prepared_data.target_frame

        # Prepare model
        model = catboost.CatBoostRegressor(
            iterations=250,
            verbose=True,
            cat_features=prepared_data.text_features,
            loss_function='LogLinQuantile',
        )
        # Fit model
        model.fit(X_train, y_train, eval_set=(X_test, y_test), verbose_eval=True)
        self.model = model
        pred = model.predict(X_test)
        # learn_pred = model.predict(X_train[:20000])
        print(f"MAPE score: {mape(y_test, pred)}")
        # print(f"MAPE learn score: {mape(y_train[:20000], learn_pred)}")
        print(model.get_feature_importance(prettified=True))

    def save_model(self):
        assert self.model is not None
        self.model.save_model(self.model_name)

    def load_model(self):
        from_file_model = catboost.CatBoostRegressor()
        from_file_model.load_model(self.model_name)
        self.model = from_file_model

    def apply_model_in_db(self, to_client=False, to_final_test=False):
        raise NotImplementedError

"""
498:	learn: 14.3153480	test: 15.4904700	best: 9.9575293 (68)	total: 35.3s	remaining: 70.8ms
499:	learn: 14.3081869	test: 15.6418279	best: 9.9575293 (68)	total: 35.4s	remaining: 0us

bestTest = 9.957529273
bestIteration = 68

Shrink model to first 69 iterations.
MAPE score: 85.71802468618407
MAPE learn score: 91.03312195296125
               Feature Id  Importances
0        p_avg_int_target    37.146744
1     program_duration_ts    15.945343
2   break_distribution_id    11.122651
3           real_week_day     8.757302
4   real_program_start_ts     7.161669
5    real_flight_start_ts     6.143604
6            p_effir_rate     4.492446
7            programme_id     3.989322
8                week_day     2.678091
9       pc_avg_int_target     1.303651
10          night_program     1.037083
11   p_blocks_per_program     0.123579
12  programme_category_id     0.057741
13  pc_blocks_per_program     0.013621
14       break_content_id     0.010213
15     programme_genre_id     0.007623
16            duration_ts     0.005946
17          pc_effir_rate     0.003370
"""

"""
-- 7 days
248:	learn: 14.9826185	test: 51.2129144	best: 9.7197348 (68)	total: 1.8s	remaining: 7.23ms
249:	learn: 14.9885634	test: 31.9327437	best: 9.7197348 (68)	total: 1.81s	remaining: 0us

bestTest = 9.719734752
bestIteration = 68

Shrink model to first 69 iterations.
MAPE score: 85.7747058702379
                  Feature Id  Importances
0           p_avg_int_target    45.472321
1                   week_day    14.466762
2       real_flight_start_ts    10.698758
3      real_program_start_ts    10.623168
4      break_distribution_id     7.634707
5        program_duration_ts     7.061991
6              real_week_day     2.309979
7               programme_id     0.656557
8               p_effir_rate     0.420230
9              night_program     0.355572
10          genre_effir_rate     0.199696
11     programme_category_id     0.025994
12      genre_avg_int_target     0.019246
13         pc_avg_int_target     0.017120
14  genre_blocks_per_program     0.014173
15     pc_blocks_per_program     0.013841
16             pc_effir_rate     0.007153
17               duration_ts     0.002382
18      p_blocks_per_program     0.000350

Process finished with exit code 0
"""

######## 14 days test
"""
248:	learn: 14.7642297	test: 162.7318871	best: 9.9052902 (68)	total: 2.23s	remaining: 8.96ms
249:	learn: 14.9025514	test: 90.7650562	best: 9.9052902 (68)	total: 2.24s	remaining: 0us

bestTest = 9.905290171
bestIteration = 68

Shrink model to first 69 iterations.
MAPE score: 88.93801811182887
                  Feature Id  Importances
0       real_flight_start_ts    32.086907
1      real_program_start_ts    23.927630
2           p_avg_int_target    23.295331
3        program_duration_ts    11.766521
4      pc_blocks_per_program     2.166407
5               p_effir_rate     1.937467
6      break_distribution_id     1.815713
7            real_week_day_2     1.607979
8              night_program     0.659743
9          pc_avg_int_target     0.472756
10              programme_id     0.114945
11      genre_avg_int_target     0.043656
12     programme_category_id     0.042490
13      p_blocks_per_program     0.019292
14          genre_effir_rate     0.018636
15             real_week_day     0.008475
16  genre_blocks_per_program     0.005029
17                  week_day     0.003714
18                week_day_2     0.003650
19               duration_ts     0.003134
20             pc_effir_rate     0.000526

Process finished with exit code 0
"""

"""
Shrink model to first 69 iterations.
MAPE score: 85.79023112395494
               Feature Id  Importances
0        p_avg_int_target    37.521813
1    real_flight_start_ts    34.732275
2   real_program_start_ts     8.309049
3   break_distribution_id     8.018438
4     program_duration_ts     7.777346
5         real_week_day_2     1.837356
6       pc_avg_int_target     0.951483
7           pc_effir_rate     0.366912
8            p_effir_rate     0.326254
9    p_blocks_per_program     0.086412
10          night_program     0.059821
11            duration_ts     0.008320
12                holiday     0.004410
13          extra_holiday     0.000110
"""