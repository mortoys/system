from tool import load_feature, merge, dump

merge([
    load_feature('ret', ['ret', 'ret_10', 'ret_22']),
    load_feature('turnover', ['r_STOM','r_STOQ','r_STOA', 'r_ATVR']),
    load_feature('size', ['r_LNCAP', 'r_MIDCAP']),
    load_feature('momentum', ['r_RSTR', 'r_STREV']),
    load_feature('volatility', ['r_BETA', 'r_HALPHA', 'r_HSIGMA', 'r_DASTD']),
    load_feature('hk_hold', ['r_HKCHG', 'r_HKCHGLN']),
]) >> dump('feature/merge')

# r_HALPHA r_RSTR r_STOM
# r_LNCAP r_STREV r_BETA