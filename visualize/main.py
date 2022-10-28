from time import sleep
from app import app
from setup import layout


if __name__ == '__main__':
    app.layout = layout
    app.run_server(debug=True)
