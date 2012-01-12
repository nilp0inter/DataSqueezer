import os
import sys
import json
import magic
from Queue import Queue
import shutil
import threading
import subprocess

class TypeExtractor(object):
    """ Detect file type and execute custom data extractors """
    def __init__(self):
        pass

class DataExtractor(threading.Thread):
    """ Custom method for data extraction """
    def __init__(self, extractors_dir, extractor_name, queue, filename):
        super(DataExtractor, self).__init__()
        self.queue = queue
        self.filename = filename
        self.extractor_name = extractor_name
        self.extractor_location = os.path.join(extractors_dir, *extractor_name.split('/'))
        self.decoder = json.JSONDecoder()

        if not os.path.isfile(self.extractor_location):
            raise ValueError('Cannot locate extractor %s' % self.extractor_location)

    def run(self):
        try:
            result_json = subprocess.check_output([ self.extractor_location,
                self.filename ], shell=True)
        except subprocess.CalledProcessError:
            # Logging
            raise

        try:
            result = self.decoder.decode(result_json)
        except:
            # Logging
            raise

        self.queue.put(result)

#
# Type extractors
#
class MimeByLibMagic(TypeExtractor):
    # http://www.iana.org/assignments/media-types/index.html
    def __init__(self):
        self.magic = magic.Magic(mime=True)

    def get_extractor(self, filename):
        extractor = self.magic.from_file(filename)
        #return extractor
        return "application/pdf2"

class MimeByExtension(TypeExtractor):
    def __init__(self):
        self.known_extensions = { \
                '.pdf' : "application/pdf", \
                '.txt' : "plain/text", \
                }

    def get_extractor(self, filename):
        fn = filename.lower()

        for extension, extractor in self.known_extensions.iteritems():
            if fn.endswith(extension):
                return extractor

        return False

class DataSqueezer(object):
    """ Extract plain data and metadata of files with a plugin-based extractors
    system.
    """
    def __init__(self, extractors_dir='extractors'):
        self.extractors = [ \
                MimeByLibMagic(), \
                MimeByExtension() \
                ]

        self.extractors_dir = extractors_dir

    def squeeze_dir(self, path, virtual_path=''):
        results = []
        for dirpath, dirnames, filenames in os.walk(path):
            if not filenames:
                continue
            else:
                for filename in filenames:
                    results.append(self.squeeze_file(fullfilename, virtual_path = virtual_path ))

        return results

    def squeeze_file(self, filename, virtual_path=''):

        if not os.path.isfile(filename):
            raise ValueError('%s is not a regular file' % filename)

        # Identify types and get extractors
        unique_extractors = set( [ type_extractor.get_extractor(filename) for type_extractor in self.extractors ] )
        result_queue = Queue()

        # Execute data_extractors in parallel
        extractors_thread = []
        for extractor in unique_extractors:
            if not extractor:
                continue
            try:
                extractors_thread.append(DataExtractor(self.extractors_dir, extractor, result_queue, filename))
                extractors_thread[-1].start()
            except ValueError, e:
                print filename, extractor

        for extractor in extractors_thread:
            extractor.join()

        accumulated_result = {}
        while not result_queue.empty():
            res = result_queue.get()
            accumulated_result.update(res)

        if virtual_path:
            virtual_filename = os.path.join(virtual_path, os.path.basename(filename) )
            accumulated_result['file'] = virtual_filename
        else:
            virtual_filename = ""
            accumulated_result['file'] = filename

        result = [ accumulated_result ]
        if accumulated_result.has_key('is_container') and \
                accumulated_result['is_container'] and \
                accumulated_result['extracted']:
            # File is a container file (zip, rar, etc...)
            result.extend(self.squeeze_dir(accumulated_result['extracted'], virtual_path=virtual_filename))
            # Enable this after test
            #shutil.rmtree(accumulated_result['extracted'])

        return result


if __name__ == '__main__':
    ds = DataSqueezer()
    print ds.squeeze_file(sys.argv[1])
