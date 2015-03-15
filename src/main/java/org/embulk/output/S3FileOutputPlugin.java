package org.embulk.output;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.IllegalFormatException;
import java.util.List;
import java.util.Locale;

import org.embulk.config.CommitReport;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Buffer;
import org.embulk.spi.Exec;
import org.embulk.spi.FileOutput;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.TransactionalFileOutput;
import org.slf4j.Logger;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;

public class S3FileOutputPlugin implements FileOutputPlugin {
    public interface PluginTask extends Task {
        @Config("path_prefix")
        public String getPathPrefix();

        @Config("file_ext")
        public String getFileNameExtension();

        @Config("sequence_format")
        @ConfigDefault("\".%03d.%02d\"")
        public String getSequenceFormat();

        @Config("bucket")
        public String getBucket();

        @Config("endpoint")
        public String getEndpoint();

        @Config("access_key_id")
        public String getAccessKeyId();

        @Config("secret_access_key")
        public String getSecretAccessKey();

        @Config("tmp_path_prefix")
        @ConfigDefault("\"embulk-output-s3-\"")
        public String getTempPathPrefix();
    }

    public static class S3FileOutput implements FileOutput,
            TransactionalFileOutput {
        private final Logger log = Exec.getLogger(S3FileOutputPlugin.class);

        private final String bucket;
        private final String pathPrefix;
        private final String sequenceFormat;
        private final String fileNameExtension;
        private final String tempPathPrefix;

        private int taskIndex;
        private int fileIndex;
        private AmazonS3Client client;
        private OutputStream current;
        private Path tempFilePath;

        public static AWSCredentialsProvider getCredentialsProvider(
                PluginTask task) {
            final AWSCredentials cred = new BasicAWSCredentials(
                    task.getAccessKeyId(), task.getSecretAccessKey());
            return new AWSCredentialsProvider() {
                @Override
                public AWSCredentials getCredentials() {
                    return cred;
                }

                @Override
                public void refresh() {
                }
            };
        }

        private static AmazonS3Client newS3Client(PluginTask task) {
            AWSCredentialsProvider credentials = getCredentialsProvider(task);

            ClientConfiguration config = new ClientConfiguration();
            // TODO: Support more configurations.

            AmazonS3Client client = new AmazonS3Client(credentials, config);
            client.setEndpoint(task.getEndpoint());

            return client;
        }

        public S3FileOutput(PluginTask task, int taskIndex) {
            this.taskIndex = taskIndex;
            this.client = newS3Client(task);
            this.bucket = task.getBucket();
            this.pathPrefix = task.getPathPrefix();
            this.sequenceFormat = task.getSequenceFormat();
            this.fileNameExtension = task.getFileNameExtension();
            this.tempPathPrefix = task.getTempPathPrefix();
        }

        private static Path newTempFile(String prefix) throws IOException {
            return Files.createTempFile(prefix, null);
        }

        private void deleteTempFile() {
            if (tempFilePath == null) {
                return;
            }

            try {
                Files.delete(tempFilePath);
                tempFilePath = null;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private String buildCurrentKey() {
            String sequence = String.format(sequenceFormat, taskIndex,
                    fileIndex);
            return pathPrefix + sequence + fileNameExtension;
        }

        private void putFile(Path from, String key) {
            PutObjectRequest request = new PutObjectRequest(bucket, key,
                    from.toFile());
            client.putObject(request);
        }

        private void closeCurrent() {
            if (current == null) {
                return;
            }

            try {
                putFile(tempFilePath, buildCurrentKey());
                fileIndex++;
            } finally {
                try {
                    current.close();
                    current = null;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                } finally {
                    deleteTempFile();
                }
            }
        }

        @Override
        public void nextFile() {
            closeCurrent();

            try {
                tempFilePath = newTempFile(tempPathPrefix);

                log.info("Writing S3 file '{}'", buildCurrentKey());

                current = Files.newOutputStream(tempFilePath);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void add(Buffer buffer) {
            if (current == null) {
                throw new IllegalStateException(
                        "nextFile() must be called before poll()");
            }

            try {
                current.write(buffer.array(), buffer.offset(), buffer.limit());
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            } finally {
                buffer.release();
            }
        }

        @Override
        public void finish() {
            closeCurrent();
        }

        @Override
        public void close() {
            closeCurrent();
        }

        @Override
        public void abort() {
            deleteTempFile();
        }

        @Override
        public CommitReport commit() {
            CommitReport report = Exec.newCommitReport();
            return report;
        }
    }

    private void validateSequenceFormat(PluginTask task) {
        try {
            @SuppressWarnings("unused")
            String dontCare = String.format(Locale.ENGLISH,
                    task.getSequenceFormat(), 0, 0);
        } catch (IllegalFormatException ex) {
            throw new ConfigException(
                    "Invalid sequence_format: parameter for file output plugin",
                    ex);
        }
    }

    @Override
    public ConfigDiff transaction(ConfigSource config, int taskCount,
            Control control) {
        PluginTask task = config.loadConfig(PluginTask.class);

        validateSequenceFormat(task);

        return resume(task.dump(), taskCount, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource, int taskCount,
            Control control) {
        control.run(taskSource);
        return Exec.newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource, int taskCount,
            List<CommitReport> successCommitReports) {
    }

    @Override
    public TransactionalFileOutput open(TaskSource taskSource, int taskIndex) {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        return new S3FileOutput(task, taskIndex);
    }
}
