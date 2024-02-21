import AWS, { DynamoDB } from 'aws-sdk';
import { sleep } from '../Api/Common';
import { ScanInput, ScanOutput } from 'aws-sdk/clients/dynamodb';
AWS.config.update({
    region: 'ap-northeast-1',
    dynamoDbCrc32: false
});
AWS.config.credentials = new AWS.Credentials('AKIAZM7CB6TBSGVR6EWN', 'nsLyA5cqh7egXn7gF4PtTO5Cks5Gg9REArmf5vGg');
const Client = new AWS.DynamoDB.DocumentClient({
    httpOptions: {
        timeout: 200,
        connectTimeout: 200
    },
    maxRetries: 10,
});
const PartiQL = new DynamoDB({
    httpOptions: {
        timeout: 200,
        connectTimeout: 200
    },
    maxRetries: 10,
});
const { unmarshall } = DynamoDB.Converter;
enum DynamoAccessType {
    PUT = 0,
    UPDATE,
    DELETE,
    QUERY,
    GET,
    SCAN,
    TRANSWRITE,
    TRANSGET,
    BATCHWRITE,
    BATCHGET
};
interface dynamoHandlerResponse {
    result: boolean;
    item: any;
    error: any;
}
export interface PutItemInput extends AWS.DynamoDB.DocumentClient.PutItemInput { }
export interface DeleteItemInput extends AWS.DynamoDB.DocumentClient.DeleteItemInput { }
export interface UpdateItemInput extends AWS.DynamoDB.DocumentClient.UpdateItemInput { }
export interface QueryInput extends AWS.DynamoDB.DocumentClient.QueryInput { }
export interface GetItemInput extends AWS.DynamoDB.DocumentClient.GetItemInput { }
export interface TransactWriteItemsInput extends AWS.DynamoDB.DocumentClient.TransactWriteItemsInput { }
export interface TransactGetItem extends AWS.DynamoDB.DocumentClient.TransactGetItem { }
export interface BatchGetItemInput extends AWS.DynamoDB.DocumentClient.BatchGetItemInput { }
export interface BatchWriteItemInput extends AWS.DynamoDB.DocumentClient.BatchWriteItemInput { }
export interface PutItemOutPut extends AWS.DynamoDB.DocumentClient.PutItemOutput { }
export interface UpdateItemOutput extends AWS.DynamoDB.DocumentClient.UpdateItemOutput { }
export interface DeleteItemOutput extends AWS.DynamoDB.DocumentClient.DeleteItemOutput { }
export interface QueryOutput extends AWS.DynamoDB.DocumentClient.QueryOutput { }
export interface GetItemOutput extends AWS.DynamoDB.DocumentClient.GetItemOutput { }
export interface TransactWriteItemsOutput extends AWS.DynamoDB.DocumentClient.TransactWriteItemsOutput { }
export interface TransactGetItemsOutput extends AWS.DynamoDB.DocumentClient.TransactGetItemsOutput { }
export interface BatchGetItemOutput extends AWS.DynamoDB.DocumentClient.BatchGetItemOutput { }
export interface BatchWriteItemOutput extends AWS.DynamoDB.DocumentClient.BatchWriteItemOutput { }

function isDynamoDbRetry(statusCode: number, code: string) {
    // 503系エラーの場合はリトライ
    if (503 === statusCode ||
        (
            // 400系エラーの場合でスロットリング系のエラーの場合はリトライ
            400 === statusCode &&
            (
                'ProvisionedThroughputExceeded' === code ||
                'ProvisionedThroughputExceededException' === code ||
                'RequestLimitExceeded' === code ||
                'ThrottlingException' === code
            )
        )) {
        return true;
    } else {
        return false;
    }
}

async function dynamoDbWrapper(type: DynamoAccessType, params: any) {

    return new Promise(async (resolve, reject) => {

        try {
            switch (type) {
                case DynamoAccessType.PUT: resolve(await Client.put(params).promise()); break;
                case DynamoAccessType.UPDATE: resolve(await Client.update(params).promise()); break;
                case DynamoAccessType.DELETE: resolve(await Client.delete(params).promise()); break;
                case DynamoAccessType.QUERY: resolve(await Client.query(params).promise()); break;
                case DynamoAccessType.GET: resolve(await Client.get(params).promise()); break;
                case DynamoAccessType.SCAN: resolve(await Client.scan(params).promise()); break;
                case DynamoAccessType.TRANSWRITE: resolve(await Client.transactWrite(params).promise()); break;
                case DynamoAccessType.TRANSGET: resolve(await Client.transactGet(params).promise()); break;
                case DynamoAccessType.BATCHWRITE: resolve(await Client.batchWrite(params).promise()); break;
                case DynamoAccessType.BATCHGET: resolve(await Client.batchGet(params).promise()); break;
                default: reject(new Error('dynamoDbWrapper unknown type'));
            }
        } catch (e) {
            reject(e);
        }
    });
}

type DynamoHandleParams = PutItemInput | DeleteItemInput | UpdateItemInput | QueryInput | GetItemInput | TransactWriteItemsInput | TransactGetItem | BatchGetItemInput | BatchWriteItemInput | ScanInput;
async function dynamoHandler(type: DynamoAccessType, params: DynamoHandleParams): Promise<dynamoHandlerResponse> {

    return new Promise(async (resolve, reject) => {

        let retry: number = 1;

        while (true) {
            try {

                const data = await dynamoDbWrapper(type, params);
                resolve({
                    result: true,
                    item: data,
                    error: null
                });
                break;

            } catch (e: any) {
                if (isDynamoDbRetry(e.statusCode, e.code)) {
                    console.log('retry', retry);
                    await sleep(retry * 10);
                } else {
                    console.log('dynamoHandler', e);
                    reject(e);
                    break;
                }
            }
            retry++;
        }
    });
}

export async function Put(params: PutItemInput): Promise<PutItemOutPut> {
    return new Promise(async (resolve, reject) => dynamoHandler(DynamoAccessType.PUT, params).then((res) => resolve(res.item)).catch((e) => reject(e)));
}

export async function Delete(params: DeleteItemInput): Promise<DeleteItemOutput> {
    return new Promise(async (resolve, reject) => dynamoHandler(DynamoAccessType.DELETE, params).then((res) => resolve(res.item)).catch((e) => reject(e)));
}

export async function Update(params: UpdateItemInput): Promise<UpdateItemOutput> {
    return new Promise(async (resolve, reject) => dynamoHandler(DynamoAccessType.UPDATE, params).then((res) => resolve(res.item)).catch((e) => reject(e)));
}

export async function Query(params: QueryInput): Promise<QueryOutput> {
    return new Promise((resolve, reject) => dynamoHandler(DynamoAccessType.QUERY, params).then((res) => resolve(res.item)).catch((e) => reject(e)));
}

export async function Get(params: GetItemInput): Promise<GetItemOutput> {
    return new Promise(async (resolve, reject) => dynamoHandler(DynamoAccessType.GET, params).then((res) => resolve(res.item)).catch((e) => reject(e)));
}

export async function TransWrite(params: TransactWriteItemsInput): Promise<TransactWriteItemsOutput> {
    return new Promise(async (resolve, reject) => dynamoHandler(DynamoAccessType.TRANSWRITE, params).then((res) => resolve(res.item)).catch((e) => reject(e)));
}

export async function TransGet(params: TransactGetItem): Promise<TransactGetItemsOutput> {
    return new Promise(async (resolve, reject) => dynamoHandler(DynamoAccessType.TRANSGET, params).then((res) => resolve(res.item)).catch((e) => reject(e)));
}

export async function BatchWrite(params: BatchWriteItemInput): Promise<BatchWriteItemOutput> {
    return new Promise(async (resolve, reject) => dynamoHandler(DynamoAccessType.BATCHWRITE, params).then((res) => resolve(res.item)).catch((e) => reject(e)));
}

export async function BatchGet(params: BatchGetItemInput): Promise<BatchGetItemOutput> {
    return new Promise(async (resolve, reject) => dynamoHandler(DynamoAccessType.BATCHGET, params).then((res) => resolve(res.item)).catch((e) => reject(e)));
}

export async function Scan(params: ScanInput): Promise<ScanOutput> {
    return new Promise(async (resolve, reject) => dynamoHandler(DynamoAccessType.SCAN, params).then((res) => resolve(res.item)).catch((e) => reject(e)));
}

export function executeStatement(params: DynamoDB.ExecuteStatementInput): Promise<any[]> {

    return new Promise(async (resolve, reject) => {

        let retry: number = 0;
        let Items: any[] = [];
        let NextToken: string = '';
        let output: DynamoDB.ExecuteStatementOutput;

        while (true) {

            try {

                if ('' === NextToken) {

                    output = await PartiQL.executeStatement({
                        ...params,
                    }).promise();

                } else {

                    output = await PartiQL.executeStatement({
                        ...params,
                        NextToken
                    }).promise();
                }

                if (output.Items && output.Items.length > 0) {
                    output.Items.map(item => unmarshall(item)).forEach((item) => Items.push(item));
                }

                if (undefined === output.NextToken) {
                    resolve(Items); break;
                } else {
                    NextToken = output.NextToken;
                }

            } catch (e: any) {

                if (isDynamoDbRetry(e.statusCode, e.code)) {
                    console.log('retry', ++retry);
                    await sleep(500);
                } else {
                    console.log('executeStatement', e);
                    reject(e);
                    break;
                }
            }

        }
    });
}
