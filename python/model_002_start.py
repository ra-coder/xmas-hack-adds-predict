import logging

import catboost
import pandas as pd
from sklearn.model_selection import train_test_split
from sqlalchemy import Boolean, Column, Float, Integer, text
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.sql import insert

from lib import AbstractTrainFlow, PreparedResult, mape


class CatboostTrainFlow(AbstractTrainFlow):
    model_name = 'model_002'

    def prepare_features(
        self,
        limit: int | None = None,
        filter_for_test: bool = False,
        table_prefix: str = 'train',
    ) -> PreparedResult:
        select_query = f""" --sql
            SELECT 
                train.*,
                train_002_features.* 
            FROM train
                join last_7_days_sampling 
                    on train.break_flight_id = last_7_days_sampling.id 
                    and for_test = FALSE
                join train_002_features on train_002_features.id = train.break_flight_id 
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
        ]
        bool_features = [
            'night_program',
        ]
        text_features = [  # enums
            'programme',
            'programme_category',
            'programme_genre',
            'break_distribution',
            'break_content'
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

    def learn(self, prepared_data: PreparedResult):
        X_train, X_test, y_train, y_test = train_test_split(
            prepared_data.features_frame,
            prepared_data.target_frame,
            test_size=0.1,
            random_state=41,
        )
        # Prepare model
        model = catboost.CatBoostRegressor(
            iterations=200,
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
bestTest = 15.91714157
bestIteration = 199

MAPE score: 190.28158065718318
              Feature Id  Importances
0   real_flight_start_ts    23.805712
1  real_program_start_ts    21.361495
2        programme_genre    14.340536
3     programme_category    13.117138
4    program_duration_ts    12.158348
5              programme     6.933843
6     break_distribution     5.193986
7            duration_ts     2.556119
8          break_content     0.524225
9          night_program     0.008598
"""