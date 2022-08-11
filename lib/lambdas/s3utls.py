from keys import KeyChain


def _test_s3_upload(self):
    import boto3
    from pathlib import Path
    from zipfile import ZipFile
    import hashlib

    bucket_id = 'Base-copies'

    # bucket_id = 'test-bucket-828729889298'

    def client_s3():
        # return session.Session().client(
        #     service_name='s3',
        #     **KeyChain.AWS_SELECTEL_S3
        # )
        return boto3.resource('s3', **KeyChain.AWS_SELECTEL_S3)

    def md5(path: str):
        hash_md5 = hashlib.md5()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def make_archive_file(path) -> str:
        log(f'Make archive for path: {path}')
        path = path if isinstance(path, Path) else Path(path)
        zip_path, _ = mkstemp()

        with ZipFile(zip_path, mode='w') as zip_file:
            if path.is_dir():
                dir_content = path.glob('**/*')
                for sub_path in dir_content:
                    if sub_path.name == '.DS_Store':
                        continue
                    relative_path = sub_path.relative_to(path)
                    zip_file.write(
                        sub_path, relative_path
                    )
                    log(f'add file: {relative_path}')
            else:
                zip_file.write(path)
                log(f'add file: {path.name}')

            log(
                'Zipped size: {}'.format(
                    pretty_file_size(
                        Path(zip_path).stat().st_size
                    )
                )
            )
        return zip_path

    class Progress:
        def __init__(self, file_path):
            self._total_bytes = Path(file_path).stat().st_size
            self._submitted_bytes = 0

        def __call__(self, _bytes):
            self._submitted_bytes += _bytes
            log(
                '{} of {}'.format(
                    pretty_file_size(self._submitted_bytes),
                    pretty_file_size(self._total_bytes),
                )
            )

    def put(s3, prefix: str, local: str, base_type: str, company: str):
        """" submit local file to s3 storage """
        pack_path = make_archive_file(local)
        key = f'{prefix}/{Path(local).name}.zip'
        log(f'S3:Put {key}')
        metadata = {
            'Base-type': base_type,
            'Company': company
        }
        s3.Bucket(bucket_id).upload_file(
            pack_path,
            Key=key,
            ExtraArgs={
                'Metadata': metadata
            },
            Callback=Progress(pack_path),
        )
        log('Done')
        assert s3.Object(bucket_id, key).e_tag[1:33] == md5(pack_path), 'Error: the checksum does not match!'

        # delete local pack file
        Path(pack_path).unlink()

    files = (
        {  # template
            'prefix': '',
            'local': '',
            'base_type': '',
            'company': ''
        },
        # {
        #     'prefix': 'DG_AUTO',
        #     'local': '/Users/igor/Downloads/Stancia/1C_DG_auto',
        #     'base_type': 'buh',
        #     'company': 'DG_AUTO'
        # },
        # {
        #     'prefix': 'Roschinskaya/zup',
        #     'local': '/Users/igor/Downloads/Stancia/1C_Roschinskaya_ZUP',
        #     'base_type': 'zup',
        #     'company': 'Roschinskaya'
        # },
        # {
        #     'prefix': 'tmp',
        #     'local': '/Users/igor/Desktop/спеки/',
        #     'base_type': 'buh',
        #     'company': 'DG_AUTO'
        # },
    )

    _s3 = client_s3()
    for f in files:
        if f['local']:  # skip template record
            put(_s3, **f)
