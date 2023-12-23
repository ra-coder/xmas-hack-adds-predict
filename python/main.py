import logging

import sshtunnel
from sqlalchemy import create_engine

from model_005_separate_models_per_program import CatboostTrainFlow as TrainFlow
logging.getLogger().setLevel(logging.INFO)


def learn_on_agent_requests():
    train_flow = TrainFlow(db_engine=engine, sampling_table_name='last_7_days_sampling')

    data = train_flow.prepare_features(for_test=None, table_name="train", program_id_filter=22)
    # test_data = train_flow.prepare_features(for_test=True, table_name="learn", program_id_filter=None)
    train_flow.learn(data)
    # train_flow.learn(data, test_prepared_data=None)
    # train_flow.save_model()
    # train_flow.load_model()
    # train_flow.apply_model_in_db(data, table_name="predict_on_learn")
    # train_flow.apply_model_in_db(test_data, table_name="predict_on_test")


def apply_to_final_test_requests():
    # support_train_flow = SupportTrainFlow(db_engine=engine)
    # support_train_flow.load_model()
    # support_train_flow.apply_model_in_db(to_final_test=True)

    # Some sQL from  0027_add_support_model_score.sql TODO move to code

    train_flow = TrainFlow(db_engine=engine, sampling_table_name='last_7_days_sampling')
    data = train_flow.prepare_features(
        for_test=None,
        table_name="test_data",
        sql_features_table_name="test_data_003_features",
    )
    train_flow.load_model()
    train_flow.apply_model_in_db(data, table_name="test_data_predict")


if __name__ == '__main__':
    with sshtunnel.open_tunnel(
            ('158.160.26.131', 22),
            ssh_username="ra-coder",
            ssh_pkey="../../../.ssh/ra-coder_ed.ppk",
            remote_bind_address=('localhost', 5432),
            local_bind_address=('localhost', 5432)
    ) as server:
        engine = create_engine(f'postgresql://xmashack:xmashack@localhost:{server.local_bind_port}/xmas_hack_adds_predict')
        logging.info('START')

        for program_id in range(1, 27):
            train_flow = TrainFlow(db_engine=engine, sampling_table_name='last_7_days_sampling')
            data = train_flow.prepare_features(for_test=None, table_name="train", program_id_filter=program_id)
            train_flow.learn(data)
            train_flow.save_model()
            train_flow.apply_model_in_db(data, table_name=f"train_predict_for_p_{program_id}")
            # data = train_flow.prepare_features(
            #     for_test=None,
            #     table_name="test_data",
            #     sql_features_table_name="test_data_003_features",
            #     program_id_filter=program_id,
            # )
            # train_flow.load_model()
            # train_flow.apply_model_in_db(data, table_name=f"test_data_predict_for_p_{program_id}")

        # apply_to_final_test_requests()
