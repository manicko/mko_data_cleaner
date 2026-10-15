import nbformat
from nbclient import NotebookClient

NB = 'examples/notebooks/mko_data_cleaner_pipeline_example.ipynb'
nb = nbformat.read(NB, as_version=4)
nbformat.validate(nb)
client = NotebookClient(nb, timeout=300, kernel_name='python3')
client.execute()
lines = ['Notebook executed OK']
for i, cell in enumerate(nb.cells):
    if cell.cell_type == 'code':
        outs = cell.get('outputs', [])
        for o in outs:
            t = o.get('output_type', '')
            if t == 'stream':
                txt = (o.get('text') or '').replace('\n', ' | ')[:300]
                lines.append(f'  cell {i} stream: {txt}')
            elif t == 'error':
                lines.append(f'  cell {i} ERROR: {o.get("ename")}: {o.get("evalue")}')
            elif t in ('execute_result', 'display_data'):
                data = o.get('data', {})
                text = data.get('text/plain', str(data))[:300]
                lines.append(f'  cell {i} result: {text}')
open('.ai/tmp/nb_outputs.txt', 'w', encoding='utf-8').write('\n'.join(lines))
print('done - wrote', len(lines), 'lines')
