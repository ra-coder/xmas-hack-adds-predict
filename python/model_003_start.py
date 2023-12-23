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
                train_003_features.* 
            FROM train
                join last_7_days_sampling 
                    on train.break_flight_id = last_7_days_sampling.id 
                    and for_test = {for_test}
                join train_003_features on train_003_features.id = train.break_flight_id 
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
            'week_day',
            'real_week_day',
        ]
        bool_features = [
            'night_program',
        ]
        text_features = [  # enums
            'programme',
            'programme_category',
            'programme_genre',
            'break_distribution',
            'break_content',
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
            iterations=10,
            verbose=True,
            cat_features=prepared_data.text_features,
            loss_function='MAPE'
        )
        # Fit model
        model.fit(X_train, y_train, eval_set=(X_test, y_test))
        self.model = model
        pred = model.predict(X_test)
        print(f"MAPE score: {mape(y_test, pred)}")
        print(model.get_feature_importance(prettified=True))

    def save_model(self):
        assert self.model is not None
        self.model.save_model(self.model_name)

    def load_model(self):
        from_file_model = catboost.CatBoostRegressor()
        from_file_model.load_model(self.model_name)
        self.model = from_file_model

    def apply_model_in_db(self, to_client=False, to_final_test=False):
        assert self.model is not None
        Session = sessionmaker(bind=self.db_engine)
        with Session() as session:
            table_name = f"{self.model_name}"
            session.execute(text(f"DROP TABLE if exists {table_name};"))
            session.execute(text(
                f"""
                    CREATE TABLE {table_name} (
                        id int primary key,
                        predict bool,
                        score float
                    );
                """
            ))
            logging.info('result table created')

            table_prefix = 'train'
            data = self.prepare_features(table_prefix=table_prefix)
            ids = data.data[['break_flight_id']]
            predicts = self.model.predict(data.features_frame)
            predict_scores = self.model.predict_proba(data.features_frame)
            logging.info('predicts calculated')

            Base = declarative_base()

            class PredictTable(Base):
                __tablename__ = table_name
                id = Column(Integer, primary_key=True)
                predict = Column(Boolean)
                score = Column(Float)

            id_with_predict_and_score = list(zip(ids['break_flight_id'], predicts, predict_scores))
            chunk_size = 10000
            for chunk in range(0, (len(id_with_predict_and_score) // chunk_size) + 1):
                if chunk * chunk_size < len(id_with_predict_and_score):
                    session.execute(
                        insert(PredictTable),
                        [
                            {'id': id_value, 'predict': predict_value == 'True', 'score': score_value[1]}
                            for id_value, predict_value, score_value
                            in id_with_predict_and_score[chunk * chunk_size:(chunk + 1) * chunk_size]
                        ],
                    )
                logging.info('saved chunk %r', chunk)
            session.commit()
            logging.info('saved to db finished')


"""
8:	learn: 0.7706226	test: 0.8246716	best: 0.8246716 (8)	total: 711ms	remaining: 79ms
9:	learn: 0.7638288	test: 0.8161778	best: 0.8161778 (9)	total: 783ms	remaining: 0us

bestTest = 0.816177798
bestIteration = 9

MAPE score: 98.97009473402866
               Feature Id  Importances
0   real_program_start_ts    61.093125
1           real_week_day    32.087724
2    real_flight_start_ts     2.598469
3             duration_ts     2.318592
4           night_program     1.152522
5                week_day     0.749569
6     program_duration_ts     0.000000
7               programme     0.000000
8      programme_category     0.000000
9         programme_genre     0.000000
10     break_distribution     0.000000
11          break_content     0.000000
"""