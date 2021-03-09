import os
import timeit

import tests_models.test_model as tm
import tests_models.time_model_load as tml

at = tm.AllTests()
at.test_clan_war()

# tml.get_insert_db_path()

# from models.model_controler import ModelControler

# N = 2
# setup = ("""
# import tests_models.time_model_load as tml
# tml.init()
# tml.fake(True)
# tml.test_db_load3(["#2R9LQRLY"])
# """)

# t = timeit.Timer("""
# tml.test_db_load1()
# """, setup=setup)
# print(t.timeit(number=N))

# t = timeit.Timer("""
# tml.test_db_load2()
# """, setup=setup)
# print(t.timeit(number=N))


# t = timeit.Timer("""
# tml.test_db_load3()
# """, setup=setup)

# print(t.timeit(number=N))
# mc = ModelControler()
# # print("len, len", mc.table_len_player(),mc.table_len_player_history())

