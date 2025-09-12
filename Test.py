import pyautogui
import time
import random

def keep_awake():
    """
    Moves the mouse slightly at random intervals to prevent sleep mode.
    Press Ctrl+C to stop the script.
    """
    print("Mouse mover started. Press Ctrl+C to stop.")
    print("The mouse will move slightly every 30-60 seconds.")
    
    try:
        while True:
            # Get current mouse position
            current_x, current_y = pyautogui.position()
            
            # Move mouse slightly (1-3 pixels in random direction)
            offset_x = random.randint(-3, 3)
            offset_y = random.randint(-3, 3)
            
            # Move to new position
            pyautogui.moveTo(current_x + offset_x, current_y + offset_y, duration=0.1)
            
            # Move back to original position to minimize disruption
            time.sleep(0.1)
            pyautogui.moveTo(current_x, current_y, duration=0.1)
            
            # Wait for a random interval between 30-60 seconds
            wait_time = random.randint(30, 60)
            print(f"Mouse moved. Next movement in {wait_time} seconds...")
            time.sleep(wait_time)
            
    except KeyboardInterrupt:
        print("\nScript stopped by user.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    # Disable pyautogui failsafe (optional - removes need to move mouse to corner to stop)
    # pyautogui.FAILSAFE = False
    
    keep_awake()