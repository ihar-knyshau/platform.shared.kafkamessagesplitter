package com.ebsco.platform.shared.streamsets;

import com.streamsets.pipeline.api.Label;

import java.util.HashMap;
import java.util.Map;

public class Groups {
    public static final String S3 = "S3";
    public static final String RS = "RS";
    public static final String MFS = "MFS";
    public static final String KAFKA = "Kafka";
    public static final String DYNAMO_DB = "DynamoDB";
    public static final String DEBEZIUM = "Debezium";
    public static final String ELASTICSEARCH = "Elasticsearch";
    public static final String MONGO = "Mongo";
    public static final String MONGO_DB = "MongoDB";
    public static final String PRODUCTS = "Products";
    public static final String FLOW = "Flow";
    public static final String FTP = "FTP";
    public static final String SSH = "SSH";
    public static final String PARAMS = "Params";
    public static final String VALIDATION = "Validation";
    public static final String SQS = "SQS";
    public static final String ATHENA = "Athena";
    public static final String CREDENTIALS = "Credentials";


    interface CustomLabel<T extends Enum> extends Label {
        Map<String, String> labels = new HashMap<>();

        default void addLabel(String name, String value) {
            labels.put(name, value);
        }

        @Override
        default String getLabel() {
            return labels.get(((T) this).name());
        }
    }

    /**
     * Simple RS group
     */
    public enum SimpleRSGroup implements CustomLabel<SimpleRSGroup> {
        RS(Groups.RS);

        SimpleRSGroup(String label) {
            addLabel(this.name(), label);
        }
    }

    /**
     * RS group
     */
    public enum RSGroup implements CustomLabel<RSGroup> {
        S3(Groups.S3),
        RS(Groups.RS);

        RSGroup(String label) {
            addLabel(this.name(), label);
        }
    }

    /**
     * Asset sender group
     */
    public enum AssetSenderGroup implements CustomLabel<AssetSenderGroup> {
        S3(Groups.S3),
        DynamoDB(Groups.DYNAMO_DB),
        RS(Groups.RS);

        AssetSenderGroup(String label) {
            addLabel(this.name(), label);
        }
    }

    /**
     * Full text group
     */
    public enum FullTextRSGroup implements CustomLabel<FullTextRSGroup> {
        Mongo(Groups.MONGO),
        RS(Groups.RS);

        FullTextRSGroup(String label) {
            addLabel(this.name(), label);
        }
    }

    /**
     * UUIDGenerator group
     */
    public enum UUIDGeneratorGroup implements CustomLabel<UUIDGeneratorGroup> {
        Params(Groups.PARAMS);

        UUIDGeneratorGroup(String label) {
            addLabel(this.name(), label);
        }
    }

    /**
     * EncoderGroup group
     */
    public enum EncoderGroup implements CustomLabel<EncoderGroup> {
        Params(Groups.PARAMS);

        EncoderGroup(String label) {
            addLabel(this.name(), label);
        }
    }

    /**
     * Products group
     */
    public enum ProductsGroup implements CustomLabel<ProductsGroup> {
        Products(Groups.PRODUCTS);

        ProductsGroup(String label) {
            addLabel(this.name(), label);
        }
    }

    /**
     * Mongo group
     */
    public enum MongoGroup implements CustomLabel<MongoGroup> {
        MongoDB(Groups.MONGO_DB);

        MongoGroup(String label) {
            addLabel(this.name(), label);
        }
    }

    public enum MongoCredentialsGroup implements CustomLabel<MongoGroup> {
        MongoDB(Groups.MONGO_DB),
        Credentials(Groups.CREDENTIALS),
        Params(Groups.PARAMS);

        MongoCredentialsGroup(String label) {
            addLabel(this.name(), label);
        }
    }

    /**
     * Debezium group
     */
    public enum DebeziumGroup implements CustomLabel<DebeziumGroup> {
        Debezium(Groups.DEBEZIUM);

        DebeziumGroup(String label) {
            addLabel(this.name(), label);
        }
    }

    /**
     * MFS group
     */
    public enum MFSGroup implements CustomLabel<MFSGroup> {
        MFS(Groups.MFS);

        MFSGroup(String label) {
            addLabel(this.name(), label);
        }
    }

    /**
     * Fulltext group
     */
    public enum FulltextGroup implements CustomLabel<FulltextGroup> {
        MFS(Groups.MFS),
        S3(Groups.S3);

        FulltextGroup(String label) {
            addLabel(this.name(), label);
        }
    }

    /**
     * S3FtpAsset group
     */
    public enum S3FtpAssetGroup implements CustomLabel<S3FtpAssetGroup> {
        MFS(Groups.MFS),
        S3(Groups.S3),
        FTP(Groups.FTP),
        SSH(Groups.SSH);

        S3FtpAssetGroup(String label) {
            addLabel(this.name(), label);
        }
    }

    /**
     * AWS group
     */
    public enum AWSGroup implements CustomLabel<AWSGroup> {
        MFS(Groups.MFS),
        S3(Groups.S3);

        AWSGroup(String label) {
            addLabel(this.name(), label);
        }
    }

    /**
     * AWS S3 group
     */
    public enum AWSS3Group implements CustomLabel<AWSS3Group> {
        S3(Groups.S3);

        AWSS3Group(String label) {
            addLabel(this.name(), label);
        }
    }

    /**
     * AWS Athena group
     */
    public enum AWSAthenaGroup implements CustomLabel<AWSAthenaGroup> {
        ATHENA(Groups.ATHENA);

        AWSAthenaGroup(String label) {
            addLabel(this.name(), label);
        }
    }

    /**
     * Kafka group for MFS
     */
    public enum KafkaMFSGroup implements CustomLabel<KafkaMFSGroup> {
        MFS(Groups.MFS),
        Kafka(Groups.KAFKA),
        Mongo(Groups.MONGO),
        S3(Groups.S3);

        KafkaMFSGroup(String label) {
            addLabel(this.name(), label);
        }
    }

    /**
     * Kafka group for RS
     */
    public enum KafkaRSGroup implements CustomLabel<KafkaRSGroup> {
        RS(Groups.RS),
        Kafka(Groups.KAFKA),
        Mongo(Groups.MONGO),
        S3(Groups.S3);

        KafkaRSGroup(String label) {
            addLabel(this.name(), label);
        }
    }

    /**
     * HTML group
     */
    public enum HTMLGroup implements CustomLabel<HTMLGroup> {
        S3(Groups.S3),
        Flow(Groups.FLOW);

        HTMLGroup(String label) {
            addLabel(this.name(), label);
        }
    }

    /**
     * MultiThreadBatch group
     */
    public enum MultiThreadBatchGroup implements CustomLabel<MultiThreadBatchGroup> {
        Flow(Groups.FLOW);

        MultiThreadBatchGroup(String label) {
            addLabel(this.name(), label);
        }
    }

    /**
     * Canonical validation group
     */
    public enum CanonicalValidationGroup implements CustomLabel<MultiThreadBatchGroup> {
        VALIDATION(Groups.VALIDATION);

        CanonicalValidationGroup(String label) {
            addLabel(this.name(), label);
        }
    }

    /**
     * SQS group
     */
    public enum SQSGroup implements CustomLabel<SQSGroup> {
        SQS(Groups.SQS);

        SQSGroup(String label) {
            addLabel(this.name(), label);
        }
    }

    /**
     * Thumbnail download group
     */
    public enum ThumbnailDownloadGroup implements CustomLabel<ThumbnailDownloadGroup> {
        Params(Groups.PARAMS);

        ThumbnailDownloadGroup(String label) {
            addLabel(this.name(), label);
        }
    }

    /**
     * Elasticsearch group
     */
    public enum ElasticsearchGroup implements CustomLabel<ElasticsearchGroup> {
        Elasticsearch(Groups.ELASTICSEARCH);

        ElasticsearchGroup(String label) {
            addLabel(this.name(), label);
        }
    }

    public enum ParamsGroup implements CustomLabel<ParamsGroup> {
        Params(Groups.PARAMS);

        ParamsGroup(String label) {
            addLabel(this.name(), label);
        }
    }

    /**
     * Simple Kafka connection group
     */
    public enum KafkaConnectionGroup implements CustomLabel<KafkaConnectionGroup> {
        Kafka(Groups.KAFKA);

        KafkaConnectionGroup(String label) {
            addLabel(this.name(), label);
        }
    }
}