# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import json
import unittest

from .streaming import ParsePaths


class ParsePathsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.parser = ParsePaths('gs://XXXX/tmp/*')
        self.test_input = """{"bucket": "XXXX", "name": "tmp/T1D10091200101309001"}"""

        media_link = 'https://www.googleapis.com/download/storage/v1/b/XXXX/o/tmp%2FT1D10091200101309001?generation' \
                     '=1635366553038121&alt=media'
        self.real_input = f"""
        {{
            "kind": "storage#object",
            "id": "XXXX/tmp/T1D10091200101309001/1635366553038121",
            "selfLink": "https://www.googleapis.com/storage/v1/b/XXXX/o/tmp%2FT1D10091200101309001",
            "name": "tmp/T1D10091200101309001",
            "bucket": "XXXX",
            "generation": "1635366553038121",
            "metageneration": "1",
            "contentType": "application/octet-stream",
            "timeCreated": "2021-10-27T20:29:13.152Z",
            "updated": "2021-10-27T20:29:13.152Z",
            "storageClass": "STANDARD",
            "timeStorageClassUpdated": "2021-10-27T20:29:13.152Z",
            "size": "9725508",
            "md5Hash": "qrMcuK4nTr9uCD7aJJqtkA==",
            "mediaLink": "{media_link}",
            "crc32c": "zKlm3w==",
            "etag": "CKna4pO36/MCEAE="
        }}"""

    def test_parse_message(self):
        actual = ParsePaths.try_parse_message(self.test_input)
        self.assertEqual(actual, {'bucket': 'XXXX', 'name': 'tmp/T1D10091200101309001'})

    def test_parse_message__already_is_dict(self):
        actual = ParsePaths.try_parse_message({'bucket': 'XXXX', 'name': 'tmp/T1D10091200101309001'})
        self.assertEqual(actual, {'bucket': 'XXXX', 'name': 'tmp/T1D10091200101309001'})

    def test_parse_message__bad_json(self):
        with self.assertRaises(json.JSONDecodeError):
            ParsePaths.try_parse_message("""{"foo": 1, "bar": 2""")

    def test_parse_message__round_trip(self):
        parsed = ParsePaths.try_parse_message(self.real_input)
        converted = json.dumps(parsed)
        re_parsed = ParsePaths.try_parse_message(converted)

        self.assertEqual(parsed, re_parsed)

    def test_should_skip(self):
        parsed = self.parser.try_parse_message(self.real_input)
        self.assertFalse(self.parser.should_skip(parsed))

    def test_should_skip__missing_values(self):
        missing_field = """{"name": "tmp/T1D10091200101309001"}"""
        parsed = self.parser.try_parse_message(missing_field)
        self.assertTrue(self.parser.should_skip(parsed))

    def test_should_skip__mismatch_pattern(self):
        self.parser = ParsePaths('gs://XXXX/foo/*')
        parsed = self.parser.try_parse_message(self.test_input)
        self.assertTrue(self.parser.should_skip(parsed))
