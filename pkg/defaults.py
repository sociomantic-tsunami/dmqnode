OPTS.update(
    # VAR.fullname includes the VAR.suffix, check the docs for details
    name = VAR.fullname,
    url = 'https://github.com/sociomantic-tsunami/dmqnode',
    maintainer = 'dunnhumby Germany GmbH <tsunami@sociomantic.com>',
    vendor = 'dunnhumby Germany GmbH',
    description = '''\
The DMQ node is a server implementing one node for a network message queue.''',
)

ARGS.extend([
    "README.rst=/usr/share/doc/{}/".format(VAR.fullname),
])
