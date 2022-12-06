import pandas as pd
import numpy as np

from tool import load_feature, dump, qrank

data = load_feature('mom', ['mom_1'])

# 最近一个月的加权累积对数日收益率
df = data.groupby('asset')['mom_1'].apply(lambda s: s.ewm(halflife=5).mean())
df = df.to_frame('STREV')

# 滞后的相对强度
df['RSTR'] = data.groupby('asset')['mom_1'].apply(lambda s: s.shift(11).ewm(halflife=126).mean())

df >> qrank >> dump('feature/momentum')

# from tool import plot
# from tool.subset import *

# plot('momentum', 'RSTR', 'main', in_main)
# plot('momentum', 'RSTR', 'kechuang', in_kechuang)
# plot('momentum', 'RSTR', 'zhongxiao', in_zhongxiao)
# plot('momentum', 'RSTR', 'chuangye', in_chuangye)

# plot('momentum', 'STREV', 'main', in_main)
# plot('momentum', 'STREV', 'kechuang', in_kechuang)
# plot('momentum', 'STREV', 'zhongxiao', in_zhongxiao)
# plot('momentum', 'STREV', 'chuangye', in_chuangye)

