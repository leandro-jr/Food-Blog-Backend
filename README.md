# Food-Blog-Backend
JetBrains Academy Project: Food Blog Backend

About
Your great-grandmother asked you to copy "to these computers" all the recipes that she had been collecting in her notebook for several decades. 
You don't like the easy ways, so you decide to build a recipe database. You will need to refresh your SQL knowledge to build a simple backend.

Learning outcomes
You will create a simple backend that will allow you to populate an SQLite3 database. 
You will know how to deal with the primary key auto-increment and how to use foreign keys to create relationships between the tables. 
Learn how to deal with many-to-many relations. Work with SQL queries and database cursor methods.

The final implementation of the project was as following:

Description
You decided to build a simple database query interface. 
The search results will be displayed on the screen, but in the future, you may want to create JSON files, load them in the frontend and show them in a browser... Stop! 
For now, the screen should suffice.

Today you want to eat a dish that will contain strawberries and milk, so you decide to build a query to the database which will return all dishes that contain both ingredients. 
Thanks to this, you will learn what else you need to buy for selected recipes. 
And since you're not interested in dinner dishes, you decide to add a second condition to find dishes that are appropriate for the time of day.

Next week you have an appointment with your great-grandmother, so you can ask about a few unreadable recipes, and maybe also show her what you have done.

Objectives
Pass two new parameters to the script: ingredients and meals. The parameters are not mandatory. 
If the new parameters are not passed, the script should work like in the previous stage.
The --ingredients parameter should contain a list of ingredients separated by commas: --ingredients="milk,sugar,tea".
The --meals parameter should contain a list of meals separated by commas --meals="dinner,supper".
You should search the database for all the recipes which contain all of the passed ingredients (recipes may contain other ingredients as well) and can be served at a specific mealtime. 
If there are recipes that meet the conditions, print their names after a colon, separated by a comma. If you find two recipes with the same name print both names.
If there are no such recipes, print: There are no such recipes in the database.
This time we will check the output, so make sure that the last line you print contains the expected elements.
You do not need to validate the arguments The tests will pass the correct values.

Example 1(data input):

> python food_blog.py food_blog.db
Pass the empty recipe name to exit.
Recipe name: > Milkshake
Recipe description: > Blend all ingredients and put in the fridge.
1) breakfast  2) brunch  3) lunch  4) supper
Enter proposed meals separated by a space: > 1 3 4
Input quantity of ingredient <press enter to stop>: > 500 ml milk
Input quantity of ingredient <press enter to stop>: > 1 cup strawberry
Input quantity of ingredient <press enter to stop>: > 1 tbsp sugar
Input quantity of ingredient <press enter to stop>: >
Pass the empty recipe name to exit.
Recipe name: > Hot cacao
Recipe description: > Pour the ingredients into the hot milk. Mix it up.
1) breakfast  2) brunch  3) lunch  4) supper
Enter proposed meals separated by a space: > 1 2
Input quantity of ingredient <press enter to stop>: > 250 ml milk
Input quantity of ingredient <press enter to stop>: > 2 tbsp cacao
Input quantity of ingredient <press enter to stop>: >
Pass the empty recipe name to exit.
Recipe name: > Hot cacao
Recipe description: > Pour the ingredients into the hot milk. Mix it up.
1) breakfast  2) brunch  3) lunch  4) supper
Enter proposed meals separated by a space: > 1 4
Input quantity of ingredient <press enter to stop>: > 250 ml milk
Input quantity of ingredient <press enter to stop>: > 2 tbsp cacao
Input quantity of ingredient <press enter to stop>: > 1 tsp sugar
Input quantity of ingredient <press enter to stop>: >
Pass the empty recipe name to exit.
Recipe name: > Fruit salad
Recipe description: > Cut strawberries and mix with other fruits. you can sprinkle everything with sugar.
1) breakfast  2) brunch  3) lunch  4) supper
Enter proposed meals separated by a space: > 3 4
Input quantity of ingredient <press enter to stop>: > 10 strawberry
Input quantity of ingredient <press enter to stop>: > 50 g black
Input quantity of ingredient <press enter to stop>: > 1 cup blue
Input quantity of ingredient <press enter to stop>: > 1 tsp sugar
Input quantity of ingredient <press enter to stop>: >
Pass the empty recipe name to exit.
Recipe name: > 
Example 2:

> python food_blog.py food_blog.db --ingredients="sugar,milk" --meals="breakfast,brunch"
Recipes selected for you: Hot cacao, Milkshake
Example 3:

> python food_blog.py food_blog.db --ingredients="sugar,milk,strawberry" --meals="brunch"
There are no such recipes in the database.
