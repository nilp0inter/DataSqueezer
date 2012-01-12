import os
import sys
import json
import Queue
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
        self.queue = queue
        self.filename = filename
        self.extractor_name = extractor_name
        self.extractor_location = os.path.join(extractors_dir, *extractor_name.split('/'))
        self.decoder = json.JSONDecoder()

        if not os.path.exists(self.extractor_location):
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
    import magic
    def __init__(self):
        self.magic = magic.Magic(mime=True)

    def get_extractor(self, filename):
        extractor = self.magic.from_file(filename)
        return extractor

class MimeByExtension(TypeExtractor):
    def __init__(self):
        self.known_extensions = { \
                '.pdf' : "application/pdf", \
                '.txt' : "plain/text", \
                }

    def get_extractor(self, filename):
        fn = filename.lower()

        for extension, extractor in self.known_extension.iteritems():
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
                    fullfilename=os.path.join(dirpath, filename)
                    res = self.squeeze_file(fullfilename, virtual_path = virtual_path )
                    results.append(res)
                    if res.has_key('is_container') and res['is_container'] and res['extracted']:
                        # File is a container file (zip, rar, etc...)
                        results.extend(self.squeeze_dir(res['extracted'], virtual_path=fullfilename)
                        # Enable this after test
                        #shutil.rmtree(res['extracted'])

        return results

    def squeeze_file(self, filename, virtual_path=''):
        unique_extractors = set( [ type_extractor.get_extractor(filename) for type_extractor in self.extractors ] )
        result_queue = Queue()

        # Execute data_extractors in parallel
        extractors_thread = []
        for extractor in unique_extractors:
            extractors_thread.append(DataExtractor(self.extractors_dir, result_queue, filename))
            extractors_thread[-1].start()

        for extractor in extractors_thread:
            extractor.join()

        accumulated_result = {}
        while not result_queue.empty():
            res = result_queue.get()
            accumulated_result.update(res)

        if virtual_path:
            accumulated_result['file'] = os.path.join(virtual_path, os.path.basename(filename) )
        else:
            accumulated_result['file'] = filename

        return accumulated_result

def __name__ == '__main__':
    ds = DataSqueezer()
    print ds.squeeze_file(sys.argv[1])
