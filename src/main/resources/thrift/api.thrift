namespace java com.leansoft.luxun.api.generated
namespace csharp Leansoft.luxun.Api.Generated

const i32 RANDOM_PARTION = -1;

const string EARLIEST_INDEX_STRING = "earliest";
const string LATEST_INDEX_STRING = "latest";

const i64 EARLIEST_TIME = -1;
const i64 LATEST_TIME = -2;

enum ResultCode
{
  SUCCESS,
  FAILURE,
  TRY_LATER
}

enum ErrorCode
{
  INTERNAL_ERROR,
  TOPIC_NOT_EXIST,
  INDEX_OUT_OF_BOUNDS,
  INVALID_TOPIC,
  TOPIC_IS_EMPTY,
  AUTHENTICATION_FAILURE,
  MESSAGE_SIZE_TOO_LARGE,
  ALL_MESSAGE_CONSUMED
}

struct Result
{
    1: required ResultCode resultCode,
    2: ErrorCode errorCode,
    3: string errorMessage
}

struct ProduceRequest {
    1: required binary item,
    2: required string topic,
}

struct ProduceResponse {
    1: required Result result,
    2: i64 index
}

struct ConsumeRequest {
    1: required string topic,
    2: string fanoutId, 
    3: i64 startIndex,
    4: i32 maxFetchSize,
}

struct ConsumeResponse {
    1: required Result result,
    2: list<binary> itemList,
    3: i64 lastConsumedIndex
}

struct FindClosestIndexByTimeRequest {
    1: required i64 timestamp,
    2: required string topic,
}

struct FindClosestIndexByTimeResponse {
    1: required Result result,
    2: i64 index,
    3: i64 timestampOfIndex
}

struct DeleteTopicRequest {
    1: required string topic,
    2: required string password
}

struct DeleteTopicResponse {
    1: required Result result,
}

struct GetSizeRequest {
    1: required string topic,
    3: string fanoutId
}

struct GetSizeResponse {
    1: required Result result,
    2: i64 size
}

service QueueService {
    ProduceResponse produce(1: ProduceRequest produceRequest);

    ConsumeResponse consume(1: ConsumeRequest consumeRequest);
    
    FindClosestIndexByTimeResponse findClosestIndexByTime(1: FindClosestIndexByTimeRequest findClosestIndexByTimeRequest);
    
    DeleteTopicResponse deleteTopic(1: DeleteTopicRequest deleteTopicRequest);
    
    GetSizeResponse getSize(1: GetSizeRequest getSizeRequest);
}