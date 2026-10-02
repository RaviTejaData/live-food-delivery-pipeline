INSERT INTO restaurants (name, cuisine, city) VALUES
  ('Spice Route',     'Indian',   'London'),
  ('Pizza Piazza',    'Italian',  'London'),
  ('Dragon Wok',      'Chinese',  'Manchester'),
  ('Burger Barn',     'American', 'Birmingham'),
  ('Sushi Wave',      'Japanese', 'London'),
  ('Taco Fiesta',     'Mexican',  'Manchester'),
  ('Green Bowl',      'Vegan',    'Leeds'),
  ('Kebab Corner',    'Turkish',  'Birmingham');

INSERT INTO riders (name, vehicle)
SELECT 'Rider ' || n,
       (ARRAY['bike','scooter','car'])[1 + floor(random()*3)::int]
FROM generate_series(1, 15) AS n;

INSERT INTO customers (name, city)
SELECT 'Customer ' || n,
       (ARRAY['London','Manchester','Birmingham','Leeds'])[1 + floor(random()*4)::int]
FROM generate_series(1, 100) AS n;
