# coding: utf-8
# for sina sae

import sae
import server

application = sae.create_wsgi_app(server.app)


