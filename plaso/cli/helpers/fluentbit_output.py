# -*- coding: utf-8 -*-
"""The Fluent Bit output module CLI arguments helper."""

from plaso.cli.helpers import interface
from plaso.cli.helpers import manager
from plaso.cli import logger
from plaso.lib import errors
from plaso.output import shared_fluentbit


class FluentBitOutputArgumentsHelper(interface.ArgumentsHelper):
  """Fluent Bit output module CLI arguments helper."""

  NAME = 'fluentbit'
  CATEGORY = 'output'
  DESCRIPTION = 'Argument helper for the Fluent Bit output modules.'

  _DEFAULT_FLUSH_INTERVAL = 1000
  _DEFAULT_PORT = 9880
  _DEFAULT_SERVER = 'localhost'

  @classmethod
  def AddArguments(cls, argument_group):
    """Adds command line arguments the helper supports to an argument group.

    This function takes an argument parser or an argument group object and adds
    to it all the command line arguments this helper supports.

    Args:
      argument_group (argparse._ArgumentGroup|argparse.ArgumentParser):
          argparse group.
    """
    argument_group.add_argument(
        '--index_name', '--index-name', dest='index_name', type=str,
        action='store', default=None, metavar='NAME',
        help='Name of the index in Fluent Bit.')

    argument_group.add_argument(
        '--flush_interval', '--flush-interval', dest='flush_interval', type=int,
        action='store', default=cls._DEFAULT_FLUSH_INTERVAL, metavar='INTERVAL',
        help='Events to queue up before bulk insert to Fluent Bit.')

    argument_group.add_argument(
        '--fluentbit-server', '--fluentbit_server', '--server', dest='server',
        type=str, action='store', default=cls._DEFAULT_SERVER,
        metavar='HOSTNAME', help=(
            'Hostname or IP address of the Fluent Bit server.'))

    argument_group.add_argument(
        '--fluentbit-port', '--fluentbit_port', '--port', dest='port',
        type=int, action='store', default=cls._DEFAULT_PORT, metavar='PORT',
        help='Port number of the Fluent Bit server.')

    argument_group.add_argument(
        '--fluentbit-url-prefix', '--fluentbit_url_prefix',
        dest='fluentbit_url_prefix', type=str, action='store', default=None,
        metavar='URL_PREFIX', help='URL prefix for Fluent Bit.')

  @classmethod
  def ParseOptions(cls, options, output_module):  # pylint: disable=arguments-renamed
    """Parses and validates options.

    Args:
      options (argparse.Namespace): parser options.
      output_module (OutputModule): output module to configure.

    Raises:
      BadConfigObject: when the output module object is of the wrong type.
      BadConfigOption: when a configuration parameter fails validation.
    """
    if not isinstance(
        output_module, shared_fluentbit.SharedFluentBitOutputModule):
      raise errors.BadConfigObject(
          'Output module is not an instance of SharedFluentBitOutputModule')

    index_name = cls._ParseStringOption(options, 'index_name')
    flush_interval = cls._ParseNumericOption(
        options, 'flush_interval', default_value=cls._DEFAULT_FLUSH_INTERVAL)

    fluentbit_url_prefix = cls._ParseStringOption(
        options, 'fluentbit_url_prefix')

    server = cls._ParseStringOption(
        options, 'server', default_value=cls._DEFAULT_SERVER)
    port = cls._ParseNumericOption(
        options, 'port', default_value=cls._DEFAULT_PORT)

    output_module.SetServerInformation(server, port)
    output_module.SetFlushInterval(flush_interval)
    output_module.SetURLPrefix(fluentbit_url_prefix)

    if index_name:
      output_module.SetIndexName(index_name)


manager.ArgumentHelperManager.RegisterHelper(FluentBitOutputArgumentsHelper)
