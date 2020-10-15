import import_airbnb
import airbnb_webapi


if __name__ == '__main__':
    import_airbnb.start()

    test_app = airbnb_webapi.app
    test_app.run()
