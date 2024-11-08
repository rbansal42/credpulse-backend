from configparser import ConfigParser

def config(filename='./database.ini', section='postgresql'):
    db = {}
    try:
        # create a parser
        parser = ConfigParser()
        
        # read config file
        parser.read(filename)

        # get section, default to postgresql
        if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                db[param[0]] = param[1]
        else:
            raise Exception(f'Section {section} not found in the {filename} file')

    except FileNotFoundError as e:
        print(f"Error: The file {filename} was not found.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if not db:
            print(f"No configuration found for section '{section}' in {filename}.")
        else:
            print(f"Configuration loaded successfully from section '{section}' in {filename}.")
        print("Finished reading configuration.")

    return db
