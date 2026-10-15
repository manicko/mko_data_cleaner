import nbformat
from nbclient import NotebookClient

NB = 'examples/notebooks/mko_data_cleaner_pipeline_example.ipynb'
nb = nbformat.read(NB, as_version=4)
nbformat.validate(nb)
client = NotebookClient(nb, timeout=300, kernel_name='python3')
client.execute()
print('Notebook executed OK')
for i, cell in enumerate(nb.cells):
    if cell.cell_type == 'code':
        outs = cell.get('outputs', [])
        for o in outs:
            t = o.get('output_type', '')
            if t == 'stream':
                txt = (o.get('text') or '')[:200].replace('\n', ' | ')
                print(f'  cell {i} stream: {txt}')
            elif t == 'error':
                print(f'  cell {i} ERROR: {o.get("ename")}: {o.get("evalue")}')
