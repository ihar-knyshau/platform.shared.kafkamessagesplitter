package com.ebsco.platform.shared.streamsets;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

import static com.ebsco.platform.shared.streamsets.ErrorTag.*;


@GenerateResourceBundle
public enum Errors implements ErrorCode {

    INCORRECT_ARTICLE_NUMBER("Incorrect article number: {}", PROCESSING),
    JSON_PARSING("JSON parsing error: {}", PROCESSING),
    INCORRECT_FILETYPE("Incorrect file type. File: {}. Expected type: {}", PROCESSING),
    BODY_EXTRACT("Body extract error: {}", PROCESSING),
    CJSON_MAPPING("C-JSON mapping error: {}", PROCESSING),
    UUID("Can't generate UUID: {}", PROCESSING),
    LONGID("Can't insert LongID into document: {}", PROCESSING),
    ENCODE("Can't encode field: {}", PROCESSING),
    NLM_TO_BITS("Can't convert NLM to BITS: {}", PROCESSING),
    NO_CJSON_FIELD_FOUND("There is no {} entry for cjson id {}", PROCESSING),
    CONFIG("A configuration is invalid because: {}", PROCESSING),
    MISSED_FIELD("Missed field in the record: {}", PROCESSING),
    STAGE("Record failed: {}", PROCESSING),
    ARTICLE_NUMBER("Article number for UI name {} in file {} can't be found in {}", PROCESSING),
    PARSE_ERROR("Cannot parse: {}", PROCESSING),
    PARSE_JATS_ERROR("Cannot parse JATS file: {}", PROCESSING),
    FILEREF_ERROR("Cannot get inputStream: {}", PROCESSING),
    BATCH_INTERRUPTED("Batch was interrupted: {}", PROCESSING),
    FIELDPATH_ERROR("Field-path error: {}", PROCESSING),
    INVALID_RECORD_FORMAT("Invalid record format: {}", PROCESSING),
    RECORD_DUPLICATED("Record duplicate found: {}", PROCESSING),
    INTERNAL_ERROR("Internal error: {}", PROCESSING),
    HTTP_ERROR("Problems with HTTP request/response: {}", PROCESSING),
    INVALID_INPUT("Invalid input data {}", PROCESSING),

    DEBEZIUM_EVENT_PARSING("Debezium parsing error: {}", MONGO),
    DEBEZIUM_EVENT_TRANSFORM("Debezium transform error: {}", MONGO),
    MONGO_UPSERT("Errors writing into MongoDB: {}", MONGO),
    MONGO_JOIN("Mongo join. Documents were not found. Size: {}; ids: {}", MONGO),
    NO_RECORDS_FOUND("No records found in db: {}, collection: {}", MONGO),
    MONGODB_DOCUMENT_NOT_FOUND("MongoDB document not found: {}", MONGO),
    MONGODB_OPLOG_LIMIT("Failed to find error in OpLog: {}", MONGO),
    MONGODB_QUERY_ERROR("MongoDB query failed: {}", MONGO),
    MONGODB_UNREACHABLE("MongoDB query failed since server is unreachable: {}", MONGO),
    MONGODB_CS_INVALID("Invalidate event received: {}", MONGO),
    MONGODB_MULTIPLE_DOCUMENTS_FOUND("Multiple documents found with '{}' field equals to {}", MONGO),

    MANIFEST_CACHE("Incorrect manifest file for path {} in bucket {}", S3),
    ETAG_CACHE("Incorrect etag file for path {}", S3),
    AWS_UPLOAD("AWS error uploading file {} in bucket {}: {}", S3),
    AWS_READ_FILE("AWS error reading file: {}", S3),
    AWS_COPY_FILE("AWS error copying file: {}", S3),
    AWS_DEBULK("AWS error on debulk stage {}", S3),
    AWS_ASSETS("Can't find/put asset with name {} for file {} in bucket {}", S3),
    S3_CLIENT_ERROR("Error sending request or handling response to S3: {}", S3),
    S3_SERVER_ERROR("S3 error: {}", S3),

    SQS_QUEUE("SQS Queue error: {}", SQS),
    SQS_NOT_FOUND("SQS Queue not found: {}", SQS),
    SQS_MESSAGE_TOO_LARGE("SQS message is too large: {}", SQS),
    SQS_CLIENT_ERROR("SQS client error: {}", SQS),
    SQS_SERVER_ERROR("SQS server error: {}", SQS),
    SQS_UKNOWN_ERROR("SQS unknown error: {}", SQS),

    BATCH_ERROR("Error while batch processing: {}", NETWORKING),
    RETRY_LIMIT("Retries limit has exceeded: {}", NETWORKING),
    INCORRECT_URI("Bad URI provided: {}", NETWORKING),
    IO("IO error: {}", NETWORKING),

    ELASTICSEARCH_4XX("Elasticsearch request failed {}", ELASTICSEARCH),
    ELASTICSEARCH_5XX("Elasticsearch request failed {}", ELASTICSEARCH),
    ELASTICSEARCH_DOCUMENT_NOT_FOUND("Elasticsearch document not found {}", ELASTICSEARCH),

    AAM_5XX("AAM service error: {}", OTHER),
    AAM_4XX("AAM client error: {}", OTHER),

    AWS("AWS error: {}", ErrorTag.AWS),
    AWS_ATHENA_ERROR("Error while processing athena query. Query id {}, message {}", ErrorTag.AWS),

    AUTH_PROBLEM("Auth problem", AUTHENTICATION),

    KAFKA("Incorrect kafka message: {}", ErrorTag.KAFKA),

    PROBLEMS_WITH_SSM("Parameter name {}. Problem: {}", SSM),

    UNDEFINED("Unknown error {}", OTHER),
    ;

    private final ErrorTag tag;
    private final String msg;

    Errors(String msg, ErrorTag tag) {
        this.tag = tag;
        this.msg = msg;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getCode() {
        return name();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getMessage() {
        return msg;
    }

    public ErrorTag getErrorTag() {
        return tag;
    }
}
