from ee_util import ConvertToCogs, WriteCogDoFn, ToEarthEngine


if __name__ == '__main__':
    input_file = '../test_data/A1D01010000010100011'
    c = ConvertToCogs('')
    w = WriteCogDoFn()
    e = ToEarthEngine()
    channel_data = c.process(input_file)

    tiff_files = []
    task_ids = []
    for i, c in enumerate(channel_data):
        print(f'{i+1} {c.name}: {c.data}')
        tiff_file = w.process(c)
        for f in tiff_file:
            tiff_files.append(f)

        print(f'EE ingesting {c.name} ...')
        task_id = e.process(c)
        for t in task_id:
            task_ids.append(t)
        
    print(f'channel data: {channel_data}')
    print(tiff_files)
    print(f'task ids: {task_ids}')
