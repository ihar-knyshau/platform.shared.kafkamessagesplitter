package com.ebsco.platform.shared.streamsets;

public class Constants {
    public static final String S3_DEVQA_INPUT_BUCKET = "${inputbucket}";
    public static final String S3_DEVQA_OUTPUT_BUCKET = "${outputbucket}";
    public static final String S3_DEVQA_CONTENT_BUCKET = "${contentbucket}";
    public static final String S3_DEVQA_DELIVERY_BUCKET = "${deliverybucket}";
    public static final String S3_DEVQA_S3DELIVERY_BUCKET = "${s3deliverybucket}";
    public static final String S3_FULL_IMAGE_FOLDER = "${fullimagefolder}";
    public static final String S3_INPUT_STORAGE = "${s3InputOld}";
    public static final String S3_INPUT_STORAGE_IMAGE_FOLDER = "${s3KeyToCleanup}";
    public static final String S3_PLACARD_IMAGE_FOLDER = "${placardimagefolder}";
    public static final String S3_AUDIOFOLDER = "${audiofolder}";
    public static final String DYNAMO_DB_UUID_TABLE = "${uuidtable}";

    public static final String S3_TMP_PATH = "TMP";

    public static final String EMPTY = "";

    public static final String KAFKA_RECORDS_TOPIC = "${kafkamfsrecordstopic}";


    public static final String INPUT_FILE_PATH_FIELD = "/input_file_path";
    public static final String DEFAULT_FIELD_VALUE = "default";

    public static final String TEXT_FIELD = "/text";

    public static final String PACKAGE_ID_FIELD = "/packageId";
    public static final String PATH_FIELD = "/filePath";
    public static final String ROOT_FOLDER_FIELD = "/rootFolder";
    public static final String OPERATION_KEY_JSON = "/operation";

    public static final String PACKAGE_ID_FIELD_JSON = "packageId";
    public static final String PATH_FIELD_JSON = "path";
    public static final String MFS_FIELD_JSON = "mfs_json";
    public static final String FILE_LINE_NUMBER_FIELD_JSON = "file_line";
    public static final String OFFSET_FIELD = "/offset";

    public static final String SQS_QUEUE_FIELD = "/queue";
    public static final String SQS_MESSAGE_FIELD = "/message";
    public static final String SQS_DELAY_FIELD = "/delay";

    public static final String OUTPUT_JSON_FIELD_NAME = "/json";
    public static final String UUID_FIELD_NAME = "/uuid";

    public static final String HTML_KEY_DEFAULT_NAME = "htmlFt";
    public static final String TOC_KEY_DEFAULT_NAME = "toc";
    public static final String FULL_TEXT_KEY_DEFAULT_NAME = "ftValue";
    public static final String CONTENT_TEMPLATE_DEFAULT_NAME = "htmlFT";
    public static final String TOC_TEMPLATE_DEFAULT_NAME = "toc";

    public static final String CONTENT_FIElD_NAME = "/file_content";
    public static final String CONTENT_EMPL_FIElD_NAME = "/file_content_empl";
    public static final String CONTENT_JATS_FIELD_NAME = "/file_content_jats";
    public static final String DETAIL_JSON_FIElD_DEFAULT_NAME = "/detailsjson";
    public static final String CONTENT_AN_FIElD_NAME = "/file_content_an";
    public static final String FULLTEXT_IMAGE_LINKS = "/fulltext_image_links";
    public static final String FULLTEXT_IMAGE_LINKS_MAP = "/fulltext_image_links_map";
    public static final String UUID_IMAGE_LINKS = "/uuid_image_links";
    public static final String IMAGE_FOLDER_LINKS = "/uuid_image_folder_links";
    public static final String FULLTEXT_HYPER_LINKS = "/fulltext_hyper_links";
    public static final String FILENAMES_LIST = "/filenames_list";
    public static final String FILES_CONTENT = "/files_content";
    public static final String CJSON_FIElD_NAME = "/cjson";
    public static final String CJSON_UUID_FIElD_NAME = "/cjson_uuid";
    public static final String CJSON_FULLTEXT_PATH_FIELD_NAME = "/fulltext_path";
    public static final String MFS_JSON_FIElD_NAME = OUTPUT_JSON_FIELD_NAME;

    public static final String S3_FILE_DESTINATION_FIElD_NAME = "/file_dest";

    public static final String CJSON = "cjson";
    public static final String FULL_TEXT_FIELD = "fullText";
    public static final String FULL_TEXT_FIELD_NAME = "/" + FULL_TEXT_FIELD;

    public static final String DYNAMO_DB_KEY_FIELD = "/dynamodb_key";
    public static final String DYNAMO_DB_VALUE_FIELD = "/dynamodb_value";
    public static final String DYNAMO_DB_UUID_KEY = "entityId";
    public static final String FILE_EXISTS_FIELD_NAME = "/file_exists";

    public static final String UUID_FULLTEXT_FIELD = "/uuidFullText";
    public static final String UUID_META_FIELD = "/uuidMeta";

    public static final String RS_PATH_FIELD = "/filePath";
    public static final String RS_DOC_FIELD = "/doc";
    public static final String RS_ETAG_FIELD = "/etag";
    public static final String RS_BITS_DOC_FIELD = "/bitsDoc";
    public static final String RS_ASSET_MAPPING_FIELD = "/assetMapping";
    public static final String RS_META_FIELD = "/meta";
    public static final String RS_MANIFEST_PATH = "${manifestpath}";
    public static final String RS_ETAG_PATH = "${etagpath}";
    public static final String RS_ARTICLE_NUMBER_FIELD = "/an";
    public static final String RS_UI_NAME_FIELD = "/ui";
    public static final String RS_NUMBER_OF_THREADS = "5";
    public static final String RS_FAIL_ON_MANIFEST_FIELD = "/manifestFail";

    public static final String ASSET_ID = "asset-id";
    public static final String ASSET_SOURCE = "asset-source";
    public static final String ASSET_MIME_TYPE = "asset-mime-type";
    public static final String ASSET_LOCATION = "asset-location";
    public static final String ASSET_REFS = "asset-refs";

    public static final String SOURCE_META = "source";

    public static final String MONGO_CONNECTION_STRING = "${mongouri}";
    public static final String MONGO_DB = "mfs";
    public static final String MONGO_DOCUMENT_COLLECTION = "fulltext";
    public static final String MONGO_COUNTER_COLLECTION = "counters";
    public static final String MONGO_COUNTER_ID = "longid";
    public static final String MONGO_COUNTER_PATH = "header[0]/long-id";
    public static final String MONGO_COUNTER_FIELD = "last-long-id";
    public static final String MONGO_DOCUMENT_FIELD = "/document_fulltext";
    public static final String MONGO_ID = "_id";
    public static final String MONGO_AUTH_DB = "admin";
    public static final String MONGO_READWRITE_USER = "readWriteUser";
    public static final String MONGO_FTKEY = "ftKey";
    public static final String MONGO_COLLECTION_FIELD = "/collection";
    public static final String MONGO_FTVALUE = FULL_TEXT_KEY_DEFAULT_NAME;
    public static final String MONGO_UUID = "uuid";
    public static final String RS_MONGO_DB = "${mongodb}";
    public static final String RS_MONGO_FULLTEXT_COLLECTION = "${mongoftcollection}";
    public static final String RS_MONGO_META_COLLECTION = "${mongometacollection}";

    public static final String HTMLFLOW_S3_DEVQA_INPUT_BUCKET = "${htmlflowinputbucket}";
    public static final String HTMLFLOW_S3_DEVQA_OUTPUT_BUCKET = "${htmlflowoutputbucket}";
    public static final String HTMLFLOW_INPUT_CJSON_FIELD_NAME = CJSON_FIElD_NAME;
    public static final String HTMLFLOW_INPUT_ID_CJSON_FIELD_NAME = "/cjson/_id";
    public static final String HTMLFLOW_INPUT_DETAILS_JSON_FIELD_NAME = DETAIL_JSON_FIElD_DEFAULT_NAME;
    public static final String HTMLFLOW_HEADER_ARRAY_FIELD_NAME = "/cjson/header";
    public static final String HTMLFLOW_STATIC_ASSETS_ARRAY_FIELD_NAME = "asset-refs";
    public static final String HTMLFLOW_ASSET_ID_FIELD_NAME = "asset-id";
    public static final String HTMLFLOW_ASSET_MIME_TYPE_FIELD_NAME = "asset-mime-type";
    public static final String HTMLFLOW_ASSET_SOURCE_FILE_PATH_FIELD_NAME = "asset-location";
    public static final String HTMLFLOW_CJSON_PREFIX = "json";
    public static final String HTMLFLOW_DETAILS_OUTPUT_FILENAME = "content";
    public static final String HTMLFLOW_DETAILS_OUTPUT_FILENAME_EXTENSION = "json";
    public static final String HTMLFLOW_DO_REAL_WORK_WITH_S3 = "${dorealworkwiths3}";
    public static final String HTMLFLOW_UPSERT_DELETE_VERSIONS = "${upsertdeleteversions}";

    public static final String FTP_LAST_MODIFICATION_TIME = "ftp_last_modification_time";
    public static final String ORIGIN_FILENAME = "origin_filename";
    public static final String CONTENT_TYPE = "content_type";
    public static final String FILES_DETAILS = "/files_details";
    public static final String DESTINATION_PATH = "destination_path";
    public static final String SOURCE_PATH = "source_path";
    public static final String ADDITIONAL_METADATA = "additional_metadata";
    public static final String FULLTEXT_OFFSET_XPATH = "{type: \"xpath\"," +
            "value:\"/body/sec//ext-link[@ext-link-type=\"image\"]/@href\"}";

    public static final String DEBEZIUM_OPERATION_PATH = "/payload/op";
    public static final String DEBEZIUM_CR_JSON_PATH = "/payload/after";
    public static final String DEBEZIUM_U_JSON_PATH = "/payload/patch";
    public static final String DEBEZIUM_CREATE_OPERATION = "c";
    public static final String DEBEZIUM_READ_OPERATION = "r";
    public static final String DEBEZIUM_RELOAD_OPERATION = "reload";
    public static final String DEBEZIUM_UPDATE_OPERATION = "u";

    public static final String OPERATION_TYPE_ATRRIBUTE = "Operation-Type";
    public static final String CREATE_OPERATION = "CREATE";
    public static final String READ_OPERATION = "READ";
    public static final String UPDATE_OPERATION = "UPDATE";

    public static final String THUMBNAIL_S3KEY_FIELDPATH = "/header[0]/asset-refs[0]/asset-location";
    public static final String THUMBNAIL_ID_FIELDPATH = "/header[0]/asset-refs[0]/asset-id";
    public static final String THUMBNAIL_MIMETYPE_FIELDPATH = "/header[0]/asset-refs[0]/asset-mime-type";
    public static final String THUMBNAIL_URL_FIELDPATH = "/header[0]/asset-refs[0]/asset-source";
    public static final String THUMBNAIL_S3_BUCKET = "s3images-collections-contentmanagement-devqa-delivery";
    public static final String THUMBNAIL_S3_FOLDER = "cxense_images";
    public static final String THUMBNAIL_EXTENSION_ATTRIBUTE = "extension";

    public static final String ERS_START_TIMESTAMP = "/rs-start-timestamp";
}