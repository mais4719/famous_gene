#!/usr/bin/env python3
import json
from collections import defaultdict

from GeneETL import Retriever
from GeneETL import get_score_calculators
from GeneETL import fetch_genes_info


if __name__ == '__main__':

    input_gene_symbols = ['TP53', 'BRCA1', 'ALK', 'RET']
    #input_gene_symbols = []
    #for row in open('/home/isaksson/Desktop/famous_gene/refseq_gene_symbols.txt'):
    #    input_gene_symbols.append(row.strip())
    genes = fetch_genes_info(input_gene_symbols)
    print('Found {} gene symbols.'.format(len(genes)))

    # Get and load score calculators
    score_calculators = get_score_calculators(config_file='gene_etl.conf')
    for sc in score_calculators:
        print('Loaded -> {}'.format(sc.name))

    #results = defaultdict(dict)
    #for sc in score_calculators:
    #    print('Running -> {}'.format(sc.name))
    #    score_generator = sc.get_scores(genes=genes)
    #    if sc.active:
    #        results[sc.name] = sc.get_scores(genes=genes)

    retriever = Retriever(score_calculators, genes)

    # Add score calculators as tasks.
    #for sc in score_calculators:

    #pending = asyncio.Task.all_tasks()
    #loop.run_until_complete(asyncio.gather(*pending))

    for source, data in retriever.results.items():
        print(source, data)

    #with open('/home/isaksson/Desktop/famous_gene/results.json', 'w') as fp:
    #    json.dump(results, fp=fp, sort_keys=True, indent=4, separators=(',', ': '))
