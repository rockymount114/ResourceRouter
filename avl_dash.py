import pandas as pd
import folium
from flask import Flask, render_template_string, request

app = Flask(__name__)

# Read the CSV file with low_memory set to False
df = pd.read_csv('avl.csv', low_memory=False)

@app.route('/', methods=['GET', 'POST'])
def index():
    vehicle_ids = df['vehicleid'].unique()  # Get unique vehicle IDs
    selected_id = None
    if request.method == 'POST':
        selected_id = request.form.get('vehicle_id')
        df_filtered = df[df['vehicleid'] == selected_id]  # Filter by selected vehicle ID

        # Create a map centered on the mean coordinates
        center_lat = df['latitude'].mean()
        center_lon = df['longitude'].mean()
        m = folium.Map(location=[center_lat, center_lon], zoom_start=12)

        # Add markers for each vehicle in the filtered DataFrame
        for _, row in df_filtered.iterrows():
            folium.Marker(
                location=[row['latitude'], row['longitude']],
                popup=f"ID: {row['vehicleid']}<br>Name: {row['name']}",
                tooltip=row['name']
            ).add_to(m)

        # Instead of saving the map, render it directly
        map_html = m._repr_html_()  # Get the HTML representation of the map
        return render_template_string('''
            <h1>Vehicle Map</h1>
            <div>{{ map_html|safe }}</div>
            <a href="/">Go back</a>
        ''', map_html=map_html)

    # Render the dropdown form
    return render_template_string('''
        <form method="post">
            <label for="vehicle_id">Select Vehicle ID:</label>
            <select name="vehicle_id" id="vehicle_id">
                {% for id in vehicle_ids %}
                    <option value="{{ id }}">{{ id }}</option>
                {% endfor %}
            </select>
            <input type="submit" value="Show on Map">
        </form>
    ''', vehicle_ids=vehicle_ids)

if __name__ == '__main__':
    app.run(debug=True)
