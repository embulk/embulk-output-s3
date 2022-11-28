/*
 * Copyright 2015 Manabu Takayama, and the Embulk project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.embulk.output.s3;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.IllegalFormatException;
import java.util.List;
import java.util.Locale;

import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Buffer;
import org.embulk.spi.Exec;
import org.embulk.spi.FileOutput;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.TransactionalFileOutput;
import org.embulk.util.config.TaskMapper;
import org.slf4j.Logger;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.embulk.util.aws.credentials.AwsCredentials;
import org.embulk.util.aws.credentials.AwsCredentialsTask;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class S3FileOutputPlugin
        implements FileOutputPlugin
{
    private static final Logger logger = LoggerFactory.getLogger(S3FileOutputPlugin.class);
    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory
            .builder()
            .addDefaultModules()
            .build();
    private static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();

    public interface PluginTask
            extends AwsCredentialsTask, Task
    {
        @Config("path_prefix")
        String getPathPrefix();

        @Config("file_ext")
        String getFileNameExtension();

        @Config("sequence_format")
        @ConfigDefault("\".%03d.%02d\"")
        String getSequenceFormat();

        @Config("bucket")
        String getBucket();

        @Config("endpoint")
        @ConfigDefault("null")
        Optional<String> getEndpoint();

        @Config("http_proxy")
        @ConfigDefault("null")
        Optional<HttpProxy> getHttpProxy();
        void setHttpProxy(Optional<HttpProxy> httpProxy);

        @Config("proxy_host")
        @ConfigDefault("null")
        Optional<String> getProxyHost();

        @Config("proxy_port")
        @ConfigDefault("null")
        Optional<Integer> getProxyPort();

        @Config("tmp_path")
        @ConfigDefault("null")
        Optional<String> getTempPath();

        @Config("tmp_path_prefix")
        @ConfigDefault("\"embulk-output-s3-\"")
        String getTempPathPrefix();

        @Config("canned_acl")
        @ConfigDefault("null")
        Optional<CannedAccessControlList> getCannedAccessControlList();

        @Config("region")
        @ConfigDefault("null")
        Optional<String> getRegion();
    }

    public static class S3FileOutput
            implements FileOutput,
            TransactionalFileOutput
    {
        private final String bucket;
        private final String pathPrefix;
        private final String sequenceFormat;
        private final String fileNameExtension;
        private final String tempPathPrefix;
        private final Optional<CannedAccessControlList> cannedAccessControlListOptional;

        private int taskIndex;
        private int fileIndex;
        private AmazonS3 client;
        private OutputStream current;
        private Path tempFilePath;
        private String tempPath = null;

        private AmazonS3 newS3Client(final PluginTask task)
        {
            Optional<String> endpoint = task.getEndpoint();
            Optional<String> region = task.getRegion();

            final AmazonS3ClientBuilder builder = AmazonS3ClientBuilder
                    .standard()
                    .withCredentials(getCredentialsProvider(task))
                    .withClientConfiguration(getClientConfiguration(task));

            // Favor the `endpoint` configuration, then `region`, if both are absent then `s3.amazonaws.com` will be used.
            if (endpoint.isPresent()) {
                if (region.isPresent()) {
                    logger.warn("Either configure endpoint or region, " +
                            "if both is specified only the endpoint will be in effect.");
                }
                builder.setEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint.get(), null));
            }
            else if (region.isPresent()) {
                builder.setRegion(region.get());
            }
            else {
                // This is to keep the AWS SDK upgrading to 1.11.x to be backward compatible with old configuration.
                //
                // On SDK 1.10.x, when neither endpoint nor region is set explicitly, the client's endpoint will be by
                // default `s3.amazonaws.com`. And for pre-Signature-V4, this will work fine as the bucket's region
                // will be resolved to the appropriate region on server (AWS) side.
                //
                // On SDK 1.11.x, a region will be computed on client side by AwsRegionProvider and the endpoint now will
                // be region-specific `<region>.s3.amazonaws.com` and might be the wrong one.
                //
                // So a default endpoint of `s3.amazonaws.com` when both endpoint and region configs are absent are
                // necessary to make old configurations won't suddenly break. The side effect is that this will render
                // AwsRegionProvider useless. And it's worth to note that Signature-V4 won't work with either versions with
                // no explicit region or endpoint as the region (inferrable from endpoint) are necessary for signing.
                builder.setEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("s3.amazonaws.com", null));
            }

            builder.withForceGlobalBucketAccessEnabled(true);
            return builder.build();
        }

        private AWSCredentialsProvider getCredentialsProvider(PluginTask task)
        {
            return AwsCredentials.getAWSCredentialsProvider(task);
        }

        private ClientConfiguration getClientConfiguration(PluginTask task)
        {
            ClientConfiguration clientConfig = new ClientConfiguration();

            clientConfig.setMaxConnections(50); // SDK default: 50
            clientConfig.setSocketTimeout(8 * 60 * 1000); // SDK default: 50*1000
            clientConfig.setRetryPolicy(PredefinedRetryPolicies.NO_RETRY_POLICY);

            // set http proxy
            // backward compatibility
            if (task.getProxyHost().isPresent()) {
                logger.warn("Configuration with \"proxy_host\" is deprecated. Use \"http_proxy.host\" instead.");
                if (!task.getHttpProxy().isPresent()) {
                    ConfigMapper configMapper = CONFIG_MAPPER_FACTORY.createConfigMapper();
                    ConfigSource configSource = CONFIG_MAPPER_FACTORY.newConfigSource();
                    configSource.set("host", task.getProxyHost().get());
                    HttpProxy httpProxy = configMapper.map(configSource, HttpProxy.class);
                    task.setHttpProxy(Optional.of(httpProxy));
                } else {
                    HttpProxy httpProxy = task.getHttpProxy().get();
                    if (httpProxy.getHost().isEmpty()) {
                        httpProxy.setHost(task.getProxyHost().get());
                        task.setHttpProxy(Optional.of(httpProxy));
                    }
                }
            }

            if (task.getProxyPort().isPresent()) {
                logger.warn("Configuration with \"proxy_port\" is deprecated. Use \"http_proxy.port\" instead.");
                HttpProxy httpProxy = task.getHttpProxy().get();
                if (!httpProxy.getPort().isPresent()) {
                    httpProxy.setPort(task.getProxyPort());
                    task.setHttpProxy(Optional.of(httpProxy));
                }
            }

            if (task.getHttpProxy().isPresent()) {
                setHttpProxyInAwsClient(clientConfig, task.getHttpProxy().get());
            }

            return clientConfig;
        }

        private void setHttpProxyInAwsClient(ClientConfiguration clientConfig, HttpProxy httpProxy) {
            // host
            clientConfig.setProxyHost(httpProxy.getHost());

            // port
            if (httpProxy.getPort().isPresent()) {
                clientConfig.setProxyPort(httpProxy.getPort().get());
            }

            // https
            clientConfig.setProtocol(httpProxy.getHttps() ? Protocol.HTTPS : Protocol.HTTP);

            // user
            if (httpProxy.getUser().isPresent()) {
                clientConfig.setProxyUsername(httpProxy.getUser().get());
            }

            // password
            if (httpProxy.getPassword().isPresent()) {
                clientConfig.setProxyPassword(httpProxy.getPassword().get());
            }
        }

        public S3FileOutput(PluginTask task, int taskIndex)
        {
            this.taskIndex = taskIndex;
            this.client = newS3Client(task);
            this.bucket = task.getBucket();
            this.pathPrefix = task.getPathPrefix();
            this.sequenceFormat = task.getSequenceFormat();
            this.fileNameExtension = task.getFileNameExtension();
            this.tempPathPrefix = task.getTempPathPrefix();
            if (task.getTempPath().isPresent()) {
                this.tempPath = task.getTempPath().get();
            }
            this.cannedAccessControlListOptional = task.getCannedAccessControlList();
        }

        private static Path newTempFile(String tmpDir, String prefix)
                throws IOException
        {
            if (tmpDir == null) {
                return Files.createTempFile(prefix, null);
            }
            else {
                return Files.createTempFile(Paths.get(tmpDir), prefix, null);
            }
        }

        private void deleteTempFile()
        {
            if (tempFilePath == null) {
                return;
            }

            try {
                Files.delete(tempFilePath);
                tempFilePath = null;
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private String buildCurrentKey()
        {
            String sequence = String.format(sequenceFormat, taskIndex,
                    fileIndex);
            return pathPrefix + sequence + fileNameExtension;
        }

        private void putFile(Path from, String key)
        {
            PutObjectRequest request = new PutObjectRequest(bucket, key, from.toFile());
            if (cannedAccessControlListOptional.isPresent()) {
                request.withCannedAcl(cannedAccessControlListOptional.get());
            }
            client.putObject(request);
        }

        private void closeCurrent()
        {
            if (current == null) {
                return;
            }

            try {
                putFile(tempFilePath, buildCurrentKey());
                fileIndex++;
            }
            finally {
                try {
                    current.close();
                    current = null;
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
                finally {
                    deleteTempFile();
                }
            }
        }

        @Override
        public void nextFile()
        {
            closeCurrent();

            try {
                tempFilePath = newTempFile(tempPath, tempPathPrefix);

                logger.info("Writing S3 file '{}'", buildCurrentKey());

                current = Files.newOutputStream(tempFilePath);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void add(Buffer buffer)
        {
            if (current == null) {
                throw new IllegalStateException(
                        "nextFile() must be called before poll()");
            }

            try {
                current.write(buffer.array(), buffer.offset(), buffer.limit());
            }
            catch (IOException ex) {
                deleteTempFile();
                throw new RuntimeException(ex);
            }
            finally {
                buffer.release();
            }
        }

        @Override
        public void finish()
        {
            closeCurrent();
        }

        @Override
        public void close()
        {
            closeCurrent();
        }

        @Override
        public void abort()
        {
            deleteTempFile();
        }

        @Override
        public TaskReport commit()
        {
            TaskReport report = CONFIG_MAPPER_FACTORY.newTaskReport();
            return report;
        }
    }

    private void validateSequenceFormat(PluginTask task)
    {
        try {
            @SuppressWarnings("unused")
            String dontCare = String.format(Locale.ENGLISH,
                    task.getSequenceFormat(), 0, 0);
        }
        catch (IllegalFormatException ex) {
            throw new ConfigException(
                    "Invalid sequence_format: parameter for file output plugin",
                    ex);
        }
    }

    @Override
    public ConfigDiff transaction(ConfigSource config, int taskCount,
            Control control)
    {
        final PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);

        validateSequenceFormat(task);

        return resume(task.toTaskSource(), taskCount, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource, int taskCount,
            Control control)
    {
        control.run(taskSource);
        return CONFIG_MAPPER_FACTORY.newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource, int taskCount,
            List<TaskReport> successTaskReports)
    {
    }

    @Override
    public TransactionalFileOutput open(TaskSource taskSource, int taskIndex)
    {
        final TaskMapper taskMapper = CONFIG_MAPPER_FACTORY.createTaskMapper();
        final PluginTask task = taskMapper.map(taskSource, PluginTask.class);

        return new S3FileOutput(task, taskIndex);
    }
}
