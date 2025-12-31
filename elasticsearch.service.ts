import {Injectable, Logger} from '@nestjs/common';
import {ConfigService} from '@nestjs/config';
import {Client} from '@elastic/elasticsearch';
import {Index} from '@elastic/elasticsearch/api/requestParams';
import PQueue from 'p-queue';
import pRetry from 'p-retry';

@Injectable()
export class ElasticsearchService extends Client {
  private logger = new Logger('Elasticsearch');
  private queue = new PQueue({concurrency: 1});

  constructor(private readonly configService: ConfigService) {
    const config = configService.getOrThrow('microservices.elasticsearch');

    super({
      node: config.node,
      auth: {username: config.username, password: config.password},
    });
  }

  async indexWithRetry(index: string, record: Record<string, any>, params?: Index) {
    this.queue
      .add(() =>
        pRetry(() => this.indexRecord(index, record, params), {
          retries: this.configService.get<number>('microservices.elasticsearch.retries') ?? 3,
          onFailedAttempt: error => {
            this.logger.error(`Indexing record failed, retrying (${error.retriesLeft} attempts left)`, error.name);
          },
        })
      )
      .then(() => {})
      .catch(() => {});
  }

  /**
   * Delete old records from ElasticSearch
   * @param index - Index
   * @param days - Number of days ago (e.g., 30 will delete month-old data)
   */
  async deleteOldRecords(index: string, days: number) {
    const now = new Date();
    now.setDate(now.getDate() - days);

    return this.deleteByQuery({
      index,
      body: {
        query: {
          bool: {
            must: [{range: {date: {lte: now}}}],
          },
        },
      },
    });
  }

  private async indexRecord(index: string, record: Record<string, any>, params?: Index) {
    return this.index({index, body: record, ...params});
  }
}
