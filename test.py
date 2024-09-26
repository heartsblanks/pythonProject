import tkinter as tk
import customtkinter as ctk

def toggle_theme():
    current_theme = ctk.get_appearance_mode()
    new_theme = "Dark" if current_theme == "Light" else "Light"
    ctk.set_appearance_mode(new_theme)

def start_installation():
    # Simulate an installation process
    progress_bar.start()
    for i in range(101):
        progress_bar.set(i)
        window.update_idletasks()
        window.after(50)  # Simulate a delay
    progress_bar.stop()

# Create the main window
window = ctk.CTk()
window.title("Installation Window")
window.geometry("400x200")

# Toggle theme button
theme_switch = ctk.CTkSwitch(window, text="Dark / Light")
theme_switch.configure(command=toggle_theme)
theme_switch.pack(pady=10)

# Installation label
installation_label = ctk.CTkLabel(window, text="Installing Application", font=("Arial", 16))
installation_label.pack()

# Progress bar
progress_bar = ctk.CTkProgressBar(window, mode="determinate")
progress_bar.pack(pady=20)

# Start installation button
start_button = ctk.CTkButton(window, text="Start Installation", command=start_installation)
start_button.pack()

# Run the GUI main loop
window.mainloop()
