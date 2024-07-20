from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import time
import os

# List of keywords
keywords = [
    "Bathroom",
    "Carpet",
    "Pillow",
    "Mat",
    "Moquette",
    "Pictorial carpet",
    "Rugs",
    "Artificial flower",
    "Hand craft",
    "Curtains",
    "Figurines",
    "Mirror",
    "Plants",
    "Painting",
    "Clock",
    "Bed service",
    "Shelf",
    "Buffet",
    "Chair",
    "Desk",
    "Dining table",
    "Furniture",
    "Office",
    "Drawer",
    "Tv stand",
    "Chandeliers",
    "Lamp",
    "Lampshade",
    "Pot",
    "Kettle",
    "Swing",
    "Swing machine",
    "Mattress",
    "AC",
    "Fan coil",
    "Fan",
    "Stove",
    "Heater",
    "Water heater",
    "Clothes rack",
    "Detergent",
    "Pet",
    "Bicycle",
    "Book",
    "Magazine",
    "Toy",
    "Musical instruments",
    "Ball",
    "Sport",
    "Climbing",
    "Camping",
    "Fishing",
    "Diving",
    "Bus",
    "Train",
    "Airplane",
    "Children bed",
    "Car seat",
    "Stroller",
    "Children clothes",
    "Shoe",
    "Belt",
    "Bag",
    "Clothing",
    "Watch",
    "Jewelry",
    "Barber",
    "Gardening",
    "Transport",
    "Building equipment",
    "Industrial machinery",
    "Tool box",
    "Cafe",
    "Restaurant",
]

# Set up Chrome options
options = Options()
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--no-sandbox")
# Uncomment if you want to run in headless mode
# options.add_argument('--headless')

# Initialize the Chrome driver
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# Create a directory to save URLs
if not os.path.exists("pexels_image_urls"):
    os.makedirs("pexels_image_urls")

# Iterate over each keyword
for keyword in keywords:
    # Open Pexels website with the search keyword
    search_url = f"https://www.pexels.com/search/{keyword}/"
    driver.get(search_url)
    time.sleep(5)  # Wait for the page to load

    image_elements = []
    scroll_pause_time = 2
    previous_count = 0

    # Scroll until we have at least 100 images or no change in image count
    while len(image_elements) < 100:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(scroll_pause_time)  # Wait to load page
        image_elements = driver.find_elements(By.CLASS_NAME, "BreakpointGrid_item__RSMyf")

        if len(image_elements) == previous_count:
            # No change in image count after scrolling
            break
        previous_count = len(image_elements)

    # File to save image URLs
    folder_name = f"pexels_image_urls/{keyword.replace(' ', '_')}.txt"
    with open(folder_name, "w") as file:
        for index, element in enumerate(image_elements[:100]):  # Get up to the first 100 images
            try:
                img_tag = element.find_element(By.TAG_NAME, "img")
                img_url = img_tag.get_attribute("src")
                if img_url:
                    file.write(f"{img_url}\n")
                    print(f"Stored URL for image_{index}.jpg: {img_url}")
            except Exception as e:
                print(f"Could not store URL for image_{index}.jpg: {e}")

# Close the driver
driver.quit()
