from engine.backup import Backup, BackupIncr

BackupIncr('tushare-daily', 'daily').dump()

BackupIncr('tushare-adj_factor', 'adj_factor').dump()

BackupIncr('tushare-daily_basic', 'daily_basic').dump()

Backup('tushare-fina_indicator', 'fina_indicator').dump()

Backup('tushare-index_daily', 'index_daily').dump()

Backup('tushare-index_dailybasic', 'index_dailybasic').dump()

BackupIncr('tushare-cb_daily', 'cb_daily').dump()

BackupIncr('tushare-moneyflow', 'moneyflow').dump()

BackupIncr('tushare-hk_hold', 'hk_hold').dump()

BackupIncr('tushare-margin_detail', 'margin_detail').dump()

Backup('tushare-limit_list', 'limit_list').dump()

Backup('tushare-index_classify', 'index_classify').dump()

Backup('opendata-industry_cons', 'industry_cons', domain='opendata').dump()

BackupIncr('opendata-industry_indicator', 'industry_indicator', domain='opendata').dump()

# Backup('jsl-cbnew-detail_hist', 'cbnew_detail_hist', 'jsl', 'last_chg_dt').dump()