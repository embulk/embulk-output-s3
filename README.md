# S3 file output plugin for Embulk

## Overview

* **Plugin type**: file output
* **Load all or nothing**: no
* **Resume supported**: yes
* **Cleanup supported**: yes

## Configuration

- **path_prefix**: prefix of target keys (string, required)
- **file_ext**: suffix of target keys (string, required)
- **sequence_format**: format for sequence part of target keys (string, default: '.%03d.%02d')
- **bucket**: S3 bucket name (string, required)
- **endpoint**: S3 endpoint login user name (string, optional)
- **access_key_id**: AWS access key id (string, required)
- **secret_access_key**: AWS secret key (string, required)
- **tmp_path_prefix**: prefix of temporary files (string, defualt: 'embulk-output-s3-')

## Example

```yaml
path_prefix: logs/out
file_ext: .csv
bucket: my-s3-bucket
endpoint: s3-us-west-1.amazonaws.com
access_key_id: ABCXYZ123ABCXYZ123
secret_access_key: AbCxYz123aBcXyZ123
formatter:
  type: csv
```


## Build

```
$ ./gradlew gem
```
