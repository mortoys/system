from tool import compute_index, dump, call
from tool import in_kechuang, in_zhongxiao, in_chuangye, in_main


drop_zero = call(lambda df: df[(df != 0)])

compute_index(in_kechuang) >> drop_zero >> dump('index/kechuang')

compute_index(in_zhongxiao) >> drop_zero >> dump('index/zhongxiao')

compute_index(in_chuangye) >> drop_zero >> dump('index/chuangye')

compute_index(in_main) >> drop_zero >> dump('index/main')