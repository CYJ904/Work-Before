import curses

def main(stdscr):
    # Clear screen
    stdscr.clear()
    
    # Get screen height and width
    height, width = stdscr.getmaxyx()

    # Calculate section heights
    result_height = height * 50 // 100
    hints_height = height * 30 // 100
    input_height = height * 20 // 100

    # Create sub-windows for each section
    result_win = stdscr.subwin(result_height, width, 0, 0)
    hints_win = stdscr.subwin(hints_height, width, result_height, 0)
    input_win = stdscr.subwin(input_height, width, result_height + hints_height, 0)

    user_input = None

    # Main loop
    while True:
        # Display results
        result_win.clear()
        result_win.addstr(0, 0, "Displaying Results (Top 50%)")
        result_win.addstr(1, 0, f"You entered: {user_input}")
        result_win.refresh()

        # Display hints
        hints_win.clear()
        hints_win.addstr(0, 0, "Displaying Hints (Middle 30%)")
        hints_win.refresh()

        # Get user input
        input_win.clear()
        input_win.addstr(0, 0, "Enter your input (Bottom 20%): ")
        input_win.refresh()

        # Capture user input
        curses.echo()  # Allow input to be visible
        user_input = input_win.getstr().decode("utf-8")
        curses.noecho()

        # Exit condition
        if user_input.lower() == 'exit':
            break

if __name__ == "__main__":
    curses.wrapper(main)
