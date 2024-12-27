import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def process_and_plot_single_csv(filename, incoming_events=None):
    """
    Reads a CSV file, averages event_time values for each timestamp, and plots the data using seaborn.
    Optionally includes incoming event data on the same graph.

    Parameters:
        filename (str): Path to the CSV file.
        incoming_events (str): Path to an optional incoming events CSV file.
    """
    # Read the primary CSV file into a DataFrame
    df = pd.read_csv(filename)

    # Ensure the column names are standardized
    df.columns = ['event_time', 'timestamp']

    plt.figure(figsize=(10, 6))

    # Plot the primary data
    sns.lineplot(data=df, x='event_time', y='timestamp', marker='o', label='outgoing')

    # If incoming events are provided, process and plot them
    if incoming_events:
        incoming_df = pd.read_csv(incoming_events)
        incoming_df.columns = ['event_time', 'timestamp']
        sns.lineplot(data=incoming_df, x='event_time', y='timestamp', marker='x', label='incoming')

    # Finalize the plot
    plt.xlabel('Timestamp')
    plt.ylabel('Average Event Time')
    plt.title('Average Event Time per Timestamp')
    plt.legend()
    plt.grid(True)
    plt.show()


process_and_plot_single_csv("outgoing_events.csv", "incoming_events.csv")