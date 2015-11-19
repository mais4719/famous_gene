import requests
import asyncio
from configparser import ConfigParser
from time import sleep
from datetime import date
from TwitterAPI import TwitterAPI
from TwitterAPI import TwitterRestPager


class ScoreCalculatorFactory(object):

    score_factories = {}

    @staticmethod
    def add_score_factory(id, score_factory):
        ScoreCalculatorFactory.score_factories.put[id] = score_factory

    @staticmethod
    def create(id, **kwargs):
        if id not in ScoreCalculatorFactory.score_factories:
            ScoreCalculatorFactory.score_factories[id] = eval('{}.Factory()'.format(id.__name__))
        return ScoreCalculatorFactory.score_factories[id].create(**kwargs)


class ScoreCalculator(object):

    active = False

    def __init__(self, **kwargs):
        self.active = kwargs.get('active', 'False').upper() == 'TRUE'

    @property
    def name(self):
        return self.__class__.__name__

    @asyncio.coroutine
    def get_scores_async(self, genes, loop):
        r = yield from loop.run_in_executor(None, self.get_scores, genes)
        return r

    def get_scores(self, genes):
        if not self.active:
            return None

        results = {}
        for gene in genes:
            results[gene.name] = self.get_score(gene)

        print('Done -> {}'.format(self.name))
        return results


class Entrez(object):

    __entrez_url = 'http://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi'

    def query(self, query_str, database, retmax=20):

        # Create url request
        request_data = {'db': database,
                        'retmode': 'json',
                        'retmax': retmax,
                        'sort': 'relevance',
                        'term': query_str}

        response = requests.get(self.__entrez_url, params=request_data)
        response.encoding = 'utf-8'

        return response.json()


class PubMed(ScoreCalculator, Entrez):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_score(self, gene):
        """
        :param gene
        :return: Numbers of PMIDs with the gene name found in the Keywords or Title/Abstract section.
        """

        # Creating search string
        keywords_str = gene.name + '[keyword] OR ' + ' OR '.join([g + '[keyword]' for g in gene.alias])
        title_abst_str = gene.name + '[Title/Abstract] OR ' + ' OR '.join([g + '[Title/Abstract]' for g in gene.alias])
        pubmed_query = '(' + keywords_str + ') OR (' + title_abst_str + ')'

        data = self.query(pubmed_query, 'pubmed')

        return int(data['esearchresult']['count'])

    class Factory:
        def create(self, **kwargs):
            return PubMed(**kwargs)


class ClinVar(ScoreCalculator, Entrez):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_score(self, gene):
        """
        :param gene
        :return: Numbers of ClinVar annotations for the gene.
        """
        data = self.query(gene.name + '[gene]', 'clinvar')
        return int(data['esearchresult']['count'])

    @property
    def name(self):
        return self.__class__.__name__

    class Factory:
        def create(self, **kwargs):
            return ClinVar(**kwargs)


class GeneticTestingNCBI(ScoreCalculator, Entrez):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_score(self, gene):
        """
        :param gene
        :return: Numbers of genetic testings registered for the gene.
        """
        data = self.query(str(gene.entrezgene) + '[geneid]', 'gtr')
        return int(data['esearchresult']['count'])

    @property
    def name(self):
        return self.__class__.__name__

    class Factory:
        def create(self, **kwargs):
            return GeneticTestingNCBI(**kwargs)


class Twitter(ScoreCalculator):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        consumer_key = kwargs.get('consumer_key', False)
        consumer_secret = kwargs.get('consumer_secret', False)
        access_token = kwargs.get('access_token', False)
        access_token_secret = kwargs.get('access_token_secret', False)

        if consumer_key and consumer_secret and access_token and access_token_secret:
            self.api = TwitterAPI(consumer_key, consumer_secret, access_token, access_token_secret)
        else:
            self.api = False

    def get_score(self, gene):
        """
        :param gene
        :return: Number of resent tweets of the gene symbol (plus likes and re-tweeks)
        """
        score = 0

        twitter_query = '{name} gene OR genomics'.format(name=gene.name)

        # REST API endpoint that closes after returning a maximum of 100 recent tweets.
        # TwitterRestPager spaces out successive requests to stay under the rate limit of 5s between calls.
        tweets = TwitterRestPager(self.api, 'search/tweets', {'q': twitter_query, 'count': 100})
        for tweet in tweets.get_iterator(wait=6):
            if 'retweeted_status' not in tweet:
                score += int(tweet['retweet_count']) + int(tweet['favorite_count']) + 1
        sleep(6)

        return score

    @property
    def name(self):
        return self.__class__.__name__

    class Factory:
        def create(self, **kwargs):
            return Twitter(**kwargs)


class WikipediaViews(ScoreCalculator):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_score(self, gene):
        """
        :param gene
        :return: Number views on Wikipedia (English page) for the past 3 months.
        """
        score = 0

        # Create url request
        url_template = 'http://stats.grok.se/json/en/{year_month}/{name}'

        # Go 3 months back.
        today = date.today()
        q_dates = []
        for i in range(0, 3):
            month = (today.month - i) % 12 if ((today.month - i) % 12) != 0 else 12
            year = today.year if month <= today.month else today.year - 1
            if month < 10:
                month = '0{}'.format(month)
            q_dates.append('{year}{month}'.format(year=year, month=month))

        for q_date in q_dates:
            response = requests.get(url_template.format(year_month=q_date, name=gene.name))
            response.encoding = 'utf-8'
            try:
                score += sum([hits for date_stamp, hits in response.json()['daily_views'].items()])
            except ValueError:
                print(response.text)
            sleep(6)

        return score

    @property
    def name(self):
        return self.__class__.__name__

    class Factory:
        def create(self, **kwargs):
            return WikipediaViews(**kwargs)


def score_calculator_classes():
    for score_calculator_class in ScoreCalculator.__subclasses__():
        yield score_calculator_class


def get_score_calculators(config_file='gene_etl.conf'):

    config = ConfigParser()
    config.read(config_file)

    score_calculators = []
    for sc in score_calculator_classes():
        kwargs = dict(config._sections[sc.__name__]) if sc.__name__ in config.sections() else {}
        score_calculators.append(ScoreCalculatorFactory.create(sc, **kwargs))

    return score_calculators
