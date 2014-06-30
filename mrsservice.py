#!/usr/bin/env python

""" 
    Collects HTTP responses from log and calculates the median size for 
    each response type. Serves up data to HTTP clients in JSON format.

"""

import os
import re
import logging
import argparse
import pyinotify 
import threading
from time import sleep
from SocketServer import ThreadingMixIn
from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler


def findmedian(values):
  """ Determines the (int) median value of an unsorted integer list """
  assert isinstance(values, list)
  llen = len(values)
  assert llen > 0
  sortedvalues = sorted(values)
  if llen % 2 == 0:
    left = sortedvalues[llen/2-1]
    right = sortedvalues[llen/2]
    return int(round(float(left+right)/2))
  else:
    return sortedvalues[(llen+1)/2-1]


class Vault(object):
  """ Global vault that contains average response sizes """
  responses = {}

  def __init__(self):
    self.responses = {}


class HTTPRequestHandler(BaseHTTPRequestHandler):
  """ HTTP request handler """

  def do_GET(self):
    """ Handles requests like /200, /301, etc. """
    if re.search('^/[0-9]{3}[/]*$', self.path) == None:
      self.send_response(403)
      self.send_header('Content-Type', 'application/json')
      self.end_headers()
      return
    
    respcode = self.path.split('/')[1]
    if not Vault.responses.has_key(respcode):
      self.send_response(404)
      self.end_headers()
      return

    respsize = Vault.responses[respcode]
    self.send_response(200)
    self.send_header('Content-Type', 'application/json')
    self.end_headers()
    d = {"median_size" : respsize} 
    self.wfile.write(d)
    return
    

class ThreadedHttpServer(ThreadingMixIn, HTTPServer):
  """ A threaded HTTP server which serves up the data stored in the Vault """

  def __init__(self, addr, port):
    allow_reuse_address = True
    self.addr = addr
    self.port = port
    self.server = HTTPServer((addr, port), HTTPRequestHandler)

  def start(self):
    logging.info("http server listening on {0}:{1}".format(self.addr, self.port))
    self.server_thread = threading.Thread(target=self.server.serve_forever()) 
    self.server_thread.daemon = True

  def stop(self):
    self.shutdown()
    self.wait()

  def shutdown(self):
    self.socket.close()
    HTTPServer.shutdown(self)

  def wait(self):
    self.server_thread.join()


class EventHandler(pyinotify.ProcessEvent):
  def __init__(self, updater):
    self.updater = updater

  def process_IN_MODIFY(self, event):
    """ We only handle IN_MODIFY events against our file """
    if event.name != self.updater.basename:
        return
    logging.debug("Noticed update, reprocessing data")
    sleep(1)
    self.updater.process()


class VaultUpdater(object):
  """ 
      A separate thread that monitors the log file and recalculates median 
      response sizes upon change. Results are stored in the global Vault. 

  """

  def __init__(self, filename):
    self.wm = pyinotify.WatchManager()
    self.notifier = pyinotify.ThreadedNotifier(self.wm, EventHandler(self))
    self.notifier.daemon = True
    self.filename = filename
    self.basename = os.path.basename(filename)
    self.workdir = os.path.dirname(filename)
    self.process()
    self.mask = pyinotify.IN_MODIFY 
    self.watch(self.filename, self.mask)
    self.watch(self.workdir, self.mask)

  def watch(self, file, mask):
    logging.info("watching {0}".format(file))
    self.wm.add_watch(file, mask)

  def start(self):
    logging.info("dispatching event notifier")
    self.notifier.start()

  def stop(self):
    self.wm.rm_watch(self.filename, self.mask)
    self.notifier.stop()

  def process(self):
    """ Processes logfile, calculates new median values, stores them in the Vault """
    logging.debug("processing {0}".format(self.filename))
    data = {}
    with open(self.filename, 'r') as f:
      for line in f.readlines():
        tokens = line.split()
        if len(tokens) == 0:
          continue
        """ Since we know the format, we won't do complicated parsing """
        respcode = tokens[-2]
        respsize = tokens[-1]
        if re.match('^[^0-9]+$', respsize):
          respsize = 0
        if not data.get(respcode):
          data[respcode] = []
        data[respcode].append(int(respsize))

    v = Vault()
    for respcode in data.keys():
      medianvalue = findmedian(data[respcode])
      logging.debug("{0} reqs have a median of {1}"
                    "".format(respcode, medianvalue))
      v.responses[respcode] = medianvalue
    Vault.responses = v.responses



if __name__ == "__main__":
  desc = 'Calculates median response sizes from log, serves them up via HTTP/JSON'
  default_host = 'localhost'
  default_port = 8888
  default_file = '/var/log/sizes.log'
  logging.basicConfig(level=logging.DEBUG, 
                      format='%(asctime)s %(levelname)-5s %(message)s')
  parser = argparse.ArgumentParser(prog='mrsservice', description=desc)
  parser.add_argument('--address', default=default_host,
                      help='address to listen on (default: {0})'
                      ''.format(default_host))
  parser.add_argument('--port', default=default_port,
                      help='port to listen on (default: {0})'
                      ''.format(default_port)) 
  parser.add_argument('--logfile', default=default_file,
                      help='logfile with responses (default: {0})'
                      ''.format(default_file)) 
  args = parser.parse_args()

  server = ThreadedHttpServer(args.address, int(args.port))
  updater = VaultUpdater(args.logfile)
  updater.start()

  try:
    server.start()
    server.wait()
  except KeyboardInterrupt:
    logging.info("shutting down server per request")

  updater.stop()

