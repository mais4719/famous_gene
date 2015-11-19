from collections import namedtuple
from collections import defaultdict
import asyncio
import mygene


class Retriever(object):

    def __init__(self, score_calculators, genes):
        self.results = defaultdict(dict)
        self.loop = asyncio.get_event_loop()
        self.loop.run_until_complete(self.run(score_calculators, genes))

    def run(self, score_calculators, genes):
        coros = []
        for sc in score_calculators:
            coros.append(self.fetch_scores(sc, genes))
        yield from asyncio.gather(*coros)

    def fetch_scores(self, score_calculator, genes):
        if score_calculator.active:
            self.results[score_calculator.name] = yield from score_calculator.get_scores_async(genes, self.loop)


Gene = namedtuple('Gene', 'name alias entrezgene')


def fetch_genes_info(gene_names):
    genes = []
    for gene in gene_names:
        try:
            gene_info = fetch_gene_info(gene)
            genes.append(gene_info)
        except ValueError:
            print('Was not able to resolve gene name/symbol {gene}.'.format(gene=gene))

    return genes


def fetch_gene_info(gene_name):

    mg = mygene.MyGeneInfo()

    # Do a generic search
    query_resp = mg.query(q=gene_name, species='human')
    if query_resp['total'] == 0:
        raise ValueError('Did not find gene name/symbol {gene_name} in MyGene\'s database.'
                         .format(gene_name=gene_name))

    for hit in query_resp['hits']:
        if 'symbol' not in hit or 'entrezgene' not in hit:
            continue
        else:
            hit_gene_name = hit['symbol']
            entrezgene = int(hit['entrezgene'])
            break

    # Do a specific search on found gene id.
    if entrezgene:
        gene_info = mg.getgene(entrezgene, fields='alias')
        alias = gene_info['alias'] if 'alias' in gene_info else []
        if type(alias) is not list:
            alias = [alias]
    else:
        alias = []

    # Check so that provided gene_name is either the gene symbol or an alias.
    if gene_name.upper() != hit_gene_name.upper():
        if gene_name.upper() not in [name.upper() for name in alias]:
            raise SystemError('Did get a hit on gene name/symbol {gene_name}, However, it do not match found '
                              'name/symbol {hit_name} or any of the found alias ({alias_str}).'
                              .format(hit_name=hit_gene_name,
                                      alias_str=', '.join(alias)))

    return Gene(name=hit_gene_name, alias=alias, entrezgene=entrezgene)
