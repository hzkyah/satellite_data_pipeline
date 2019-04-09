import explicit as loader

g = loader.GCDownload(start='2013-01-01', end='2019-01-31', sat=8, path='44', row='34', output_path='path/to/place')
g.download()
