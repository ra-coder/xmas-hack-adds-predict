import logging

import sshtunnel
from sqlalchemy import create_engine

from model_001_start import CatboostTrainFlow001 as TrainFlow
logging.getLogger().setLevel(logging.INFO)


def learn_on_agent_requests():
    train_flow = TrainFlow(db_engine=engine, sampling_table_name='last_7_days_sampling')

    data = train_flow.prepare_features(filter_for_test=True)
    # data = train_flow.prepare_features(filter_for_test=False)
    train_flow.learn(data)
    # train_flow.save_model()
    # train_flow.load_model()
    # train_flow.apply_model_in_db()


def apply_to_final_test_requests():
    # support_train_flow = SupportTrainFlow(db_engine=engine)
    # support_train_flow.load_model()
    # support_train_flow.apply_model_in_db(to_final_test=True)

    # Some sQL from  0027_add_support_model_score.sql TODO move to code

    train_flow = TrainFlow(db_engine=engine)
    train_flow.load_model()
    train_flow.apply_model_in_db(to_final_test=True)


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

        learn_on_agent_requests()

        # apply_to_final_test_requests()
