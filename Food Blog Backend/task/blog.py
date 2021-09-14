import argparse
import sqlite3

def create_tables(conn):
    """Creates the tables"""
    cur = conn.cursor()
    cur.execute("PRAGMA foreign_keys = ON;")

    # creates table meals
    cur.execute("""CREATE TABLE IF NOT EXISTS meals (
                meal_id INTEGER PRIMARY KEY,
                meal_name TEXT UNIQUE NOT NULL);""")
    conn.commit()

    # creates table ingredients
    cur.execute("""CREATE TABLE IF NOT EXISTS ingredients (
                   ingredient_id INTEGER PRIMARY KEY,
                   ingredient_name TEXT UNIQUE NOT NULL);""")
    conn.commit()

    # creates table measures
    cur.execute("""CREATE TABLE IF NOT EXISTS measures (
                       measure_id INTEGER PRIMARY KEY,
                       measure_name TEXT UNIQUE);""")
    conn.commit()

    # creates table recipes
    cur.execute("""CREATE TABLE IF NOT EXISTS recipes (
                          recipe_id INTEGER PRIMARY KEY AUTOINCREMENT,
                          recipe_name TEXT NOT NULL,
                          recipe_description TEXT);""")
    conn.commit()

    # creates cross reference table serve between recipes and meals
    cur.execute("""CREATE TABLE IF NOT EXISTS serve (
                              serve_id INTEGER PRIMARY KEY,
                              recipe_id INTEGER NOT NULL,
                              meal_id INTEGER NOT NULL,
                              FOREIGN KEY(recipe_id) REFERENCES recipes(recipe_id)                         
                              FOREIGN KEY (meal_id) REFERENCES meals (meal_id));""")
    conn.commit()

    # creates cross reference table quantity among measures, ingredients and recipes
    cur.execute("""CREATE TABLE IF NOT EXISTS quantity (
                                  quantity_id INTEGER PRIMARY KEY,                                 
                                  measure_id INTEGER NOT NULL,
                                  ingredient_id INTEGER NOT NULL,
                                  quantity INTEGER NOT NULL,
                                  recipe_id INTEGER NOT NULL,
                                  FOREIGN KEY (measure_id) REFERENCES measures (measure_id),
                                  FOREIGN KEY (ingredient_id) REFERENCES ingredients (ingredient_id),
                                  FOREIGN KEY (recipe_id) REFERENCES recipes (recipe_id));""")
    conn.commit()

def populate_tables(conn):
    """Populates tables with info from data"""
    cur = conn.cursor()
    data = {"meals": ("breakfast", "brunch", "lunch", "supper"),
        "ingredients": ("milk", "cacao", "strawberry", "blueberry", "blackberry", "sugar"),
        "measures": ("ml", "g", "l", "cup", "tbsp", "tsp", "dsp", "")}

    for key, value in data.items():
        for index, object in enumerate(value):
            cur.execute(f"INSERT INTO {key} VALUES ({index + 1}, '{object}');")
            conn.commit()


def populate_quantity(conn, last_row_id, quantity, measure, ingredient):
    """Verify if info provided by user matches measures and ingredients tables and
     insert id's on cross table quantities"""
    cur = conn.cursor()
    if measure == '':
        cur.execute(f"SELECT * FROM measures WHERE measure_name = '';")
    else:
        cur.execute(f"SELECT * FROM measures WHERE measure_name LIKE '%{measure}%';")
    measure_query = cur.fetchall()
    cur.execute(f"SELECT * FROM ingredients WHERE ingredient_name LIKE '%{ingredient}%';")
    ingredient_query = cur.fetchall()

    if (measure_query and ingredient_query):
        if len(measure_query) == 1 and len(ingredient_query) == 1:
            measure_id = measure_query[0][0]
            ingredient_id = ingredient_query[0][0]
            measure = measure_query[0][1]
            ingredient = ingredient_query[0][1]

            cur.execute(f"""INSERT INTO quantity (measure_id, ingredient_id, quantity, recipe_id)
                        VALUES ({measure_id}, {ingredient_id}, {quantity}, {last_row_id});""")
            conn.commit()
        else:
            print("The measure is not conclusive!")
    else:
        print("The measure is not conclusive!")


def quantity_of_ingredients(conn, last_row_id):
    """Ask user about the ingredients and quantities and calls populate_quantity to fill quantity table. """
    while True:
        quant_meas_ingr = input("Input quantity of ingredient <press enter to stop>: ").split()
        # exit loop if user press enter
        if len(quant_meas_ingr) == 0:
            break
        elif len(quant_meas_ingr) == 2:
            (quantity, ingredient) = quant_meas_ingr
            measure = ''
        else:
            (quantity, measure, ingredient) = quant_meas_ingr

        populate_quantity(conn, last_row_id, quantity, measure, ingredient)


def recipes_table(conn):
    """Populates recipes table with info provided by the user.
    Populates cross reference table serve with meals and recipes ids"""
    cur = conn.cursor()
    print("Pass the empty recipe name to exit.")
    while True:
        recipe_name = input("Recipe name: ")
        if recipe_name == "":
            break
        else:
            # insert recipe in recipes table. last_row_id is the id of the last recipe added
            recipe_description = input("Recipe description: ")
            last_row_id = cur.execute(f"""INSERT INTO recipes (recipe_name, recipe_description)
            VALUES ('{recipe_name}', '{recipe_description}');""").lastrowid
            conn.commit()

            # print menu with the meals options
            cur.execute(f"SELECT * FROM meals;")
            meals_table = cur.fetchall()
            message = ""
            if meals_table:
                for row in meals_table:
                    message += f"{row[0]}) {row[1]} "
                print(message)

            # creates list with meals id's as integers
            meals_choices_string = input("When the dish can be served: ").split()
            meals_choices = [int(i) for i in meals_choices_string]

            # insert meal_id and last_row_id on serve table
            for meal_id in meals_choices:
                cur.execute(f"""INSERT INTO serve (recipe_id, meal_id) 
                VALUES ({last_row_id}, {meal_id});""")
                conn.commit()

            quantity_of_ingredients(conn, last_row_id)


def search_tables(conn, ingredients_search, meals_search):
    """Search the database for all the recipes which contain all of the passed ingredients
    (recipes may contain other ingredients as well) and can be served at a specific mealtime.
    If there are recipes that meet the conditions, print their names after a colon, separated by a comma.
    If finds two recipes with the same name print both names."""
    cur = conn.cursor()

    proceed = 1
    if ingredients_search != "":
        # creates a tuple with the ingredients id's that match the provided by the user
        if len(ingredients_search) == 1:
            cur.execute(f"SELECT ingredient_id FROM ingredients WHERE ingredient_name = '{ingredients_search[0]}';")
        else:
            cur.execute(f"SELECT ingredient_id FROM ingredients WHERE ingredient_name IN {ingredients_search};")
        ingredients_query = cur.fetchall()
        ingredients_list = []
        for row in ingredients_query:
            ingredients_list.append(row[0])
        ingredients_tuple = tuple(ingredients_list)

        # in case the user provided an ingredient that is not on the ingredients table the result should be proceed = 0
        if len(ingredients_tuple) != len(ingredients_search):
            proceed = 0
            recipes_id_tuple = ""
            recipe_meals_tuple = ""
            return proceed, recipes_id_tuple, recipe_meals_tuple

        # Search on quantity table for the recipe_id if only one ingredient was provided
        if len(ingredients_tuple) == 1:
            cur.execute(f"SELECT recipe_id FROM quantity WHERE ingredient_id = '{ingredients_tuple[0]}';")
            quantity_query = cur.fetchall()
            recipes_id_list = []
            for row in quantity_query:
                recipes_id_list.append(row[0])
            recipes_id_tuple = tuple(recipes_id_list)

        # Search on quantity table for the recipe_id if more than one ingredient was provided.
        # The recipe_id will be added to the tuple only if all of them are used on the recipe.
        else:
            cur.execute(f"SELECT recipe_id FROM quantity WHERE ingredient_id IN {ingredients_tuple};")
            quantity_query = cur.fetchall()
            recipes_id_list = []
            recipe_id = ""
            condition = len(ingredients_tuple)
            count = 1
            for row in quantity_query:
                if row[0] == recipe_id:
                    count += 1
                    if count == condition:
                        recipes_id_list.append(row[0])
                else:
                    recipe_id = row[0]
                    count = 1
            recipes_id_tuple = tuple(recipes_id_list)
    else:
        proceed = 0
        recipes_id_tuple = ""
        recipe_meals_tuple = ""
        return proceed, recipes_id_tuple, recipe_meals_tuple

    if meals_search != "":
        # search a tuple with meals id on meals table for match with --meals provided by user
        if len(meals_search) == 1:
            cur.execute(f"SELECT meal_id FROM meals WHERE meal_name = '{meals_search[0]}';")
        else:
            cur.execute(f"SELECT meal_id FROM meals WHERE meal_name IN {meals_search};")
        meals_query = cur.fetchall()
        meals_list = []
        for row in meals_query:
            meals_list.append(row[0])
        meals_tuple = tuple(meals_list)

        # search for recipe_id on serve table contain the meals_id
        if len(meals_tuple) == 1:
            cur.execute(f"SELECT recipe_id FROM serve WHERE meal_id = '{meals_tuple[0]}';")
        else:
            cur.execute(f"SELECT recipe_id FROM serve WHERE meal_id IN {meals_tuple};")

        serve_query = cur.fetchall()
        recipes_meals_list = []
        for row in serve_query:
            recipes_meals_list.append(row[0])
        recipe_meals_tuple = tuple(set(recipes_meals_list))
        return proceed, recipes_id_tuple, recipe_meals_tuple
    else:
        proceed = 0
        recipes_id_tuple = ""
        recipe_meals_tuple = ""
        return proceed, recipes_id_tuple, recipe_meals_tuple


def matched_recipes(conn, ingredients_search, meals_search):
    """ using the recipe_id obtained from quantity table and recipe_id obtained from serve table, add to the list if all
    ingredients are used on any meal time provided by user"""
    cur = conn.cursor()
    # calls search tables to obtain the recipes ids
    proceed, recipes_id_tuple, recipe_meals_tuple = search_tables(conn, ingredients_search, meals_search)
    if proceed == 0:
        print("There are no such recipes in the database.")
    else:
        recipe_final_query = []
        for recipe_id in recipes_id_tuple:
            if len(recipe_meals_tuple) == 1:
                cur.execute(f"SELECT recipe_name FROM recipes WHERE recipe_id = {recipe_id} AND recipe_id = {recipe_meals_tuple[0]};")
            else:
                cur.execute(f"SELECT recipe_name FROM recipes WHERE recipe_id = {recipe_id} AND recipe_id IN {recipe_meals_tuple};")

            recipe_final_query.append(cur.fetchone())
        if recipe_final_query[0] !=  None:
            recipe_final_string = ""
            for row in recipe_final_query:
                recipe_final_string += row[0] + ', '
            print(f"Recipes selected for you: {recipe_final_string[:-2]}")
        else:
            print("There are no such recipes in the database.")


def print_tables(conn):
    """Prints tables for verification"""
    cur = conn.cursor()
    tables = ['meals', 'ingredients', 'measures', 'recipes', 'serve', 'quantity']
    for table in tables:
        print(table)
        cur.execute(f"SELECT * FROM '{table}';")
        print_table = cur.fetchall()
        if print_table:
            for row in print_table:
                print(row)
        print()


def main():
    """Create the database, call functions to create and populate tables, return the recipes found based on
     ingredients and meal provide  by user and print tables for verification"""

    parser = argparse.ArgumentParser(description="""User should enter the database name.db and
     optional --ingredients='nono,nono...' and --meals='no,nonono,...'""")

    parser.add_argument("data_base_name")
    parser.add_argument("--ingredients")
    parser.add_argument("--meals")

    args = parser.parse_args()

    # data base name
    data_base_name = args.data_base_name
    conn = sqlite3.connect(data_base_name)

    # ingredients and meal parameters
    ingredients_args = args.ingredients
    ingredients_search = ""
    meals_search = ""

    if ingredients_args:
        ingredients_search = tuple(ingredients_args.split(','))

    meals_args = args.meals
    if meals_args:
        meals_search = tuple(meals_args.split(','))

    if ingredients_args or meals_args:
        matched_recipes(conn, ingredients_search, meals_search)
    else:
        create_tables(conn)
        populate_tables(conn)
        recipes_table(conn)
    # print_tables(conn)
    conn.close()

if __name__ == '__main__':
    main()
