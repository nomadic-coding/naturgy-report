import pandas as pd
from datetime import timedelta
from tabulate import tabulate
import locale

# Set locale to Spanish
locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')  # This might vary depending on your system

# Cargar los datos
data = pd.read_csv('filtered_output.csv')

# Función para convertir la cadena de duración a timedelta
def parse_duration(duration_str):
    parts = duration_str.split(', ')
    total_seconds = 0
    for part in parts:
        if 'h' in part:
            total_seconds += int(part.replace('h', '')) * 3600
        elif 'm' in part:
            total_seconds += int(part.replace('m', '')) * 60
        elif 's' in part:
            total_seconds += int(part.replace('s', ''))
    return timedelta(seconds=total_seconds)

# Añadir una nueva columna con duraciones analizadas
data['Duration'] = data['Duration'].apply(parse_duration)

# Convertir Timestamp a datetime
data['Timestamp'] = pd.to_datetime(data['Timestamp'])

# Filtrar datos entre octubre 2023 y septiembre 2024
start_filter_date = pd.Timestamp('2023-10-01')
end_filter_date = pd.Timestamp('2024-09-30')
data = data[(data['Timestamp'] >= start_filter_date) & (data['Timestamp'] <= end_filter_date)]

# Número total de cortes
total_outages = len(data)

# Duración promedio de los cortes en horas
average_duration = data['Duration'].mean().total_seconds() / 3600

# Corte más largo
longest_outage = data['Duration'].max().total_seconds() / 3600

# Mes más afectado
monthly_duration = data.groupby(data['Timestamp'].dt.to_period('M'))['Duration'].sum().apply(lambda x: x.total_seconds() / 3600)
most_affected_month = monthly_duration.idxmax()
most_affected_month_duration = monthly_duration.max()

# Semana más afectada
weekly_duration = data.groupby(data['Timestamp'].dt.to_period('W'))['Duration'].sum().apply(lambda x: x.total_seconds() / 3600)
most_affected_week = weekly_duration.idxmax()
most_affected_week_duration = weekly_duration.max()

# Frecuencia de cortes por mes
monthly_outage_count = data.groupby(data['Timestamp'].dt.to_period('M')).size()
average_monthly_outages = round(monthly_outage_count.mean())  # Redondeado al número entero más cercano

# Frecuencia de cortes por semana
weekly_outage_count = data.groupby(data['Timestamp'].dt.to_period('W')).size()
average_weekly_outages = round(weekly_outage_count.mean())  # Redondeado al número entero más cercano

# Calcular el corte acumulativo más largo en un período de 48 horas
data = data.sort_values(by='Timestamp')
max_48hr_outage = timedelta(0)
start_index = 0

for end_index in range(len(data)):
    while data['Timestamp'].iloc[end_index] - data['Timestamp'].iloc[start_index] > timedelta(hours=48):
        start_index += 1
    current_48hr_outage = data['Duration'].iloc[start_index:end_index + 1].sum()
    if current_48hr_outage > max_48hr_outage:
        max_48hr_outage = current_48hr_outage

max_48hr_outage_hours = max_48hr_outage.total_seconds() / 3600

# Duración total de todos los cortes en horas
total_duration_hours = data['Duration'].sum().total_seconds() / 3600

# Calcular el número de días con 1 corte y 2 o más cortes
daily_outage_count = data.groupby(data['Timestamp'].dt.date).size()

# Imprimir resultados
print(f"Analysis Period: {start_filter_date.strftime('%B %d, %Y')} to {end_filter_date.strftime('%B %d, %Y')}")
print(f"Número Total de Fallas: {total_outages} (fallas mayores de 2 minutos)")
print(f"Duración Promedio de las Fallas (en horas): {average_duration:.2f}")
print(f"Falla Más Larga (en horas): {longest_outage:.2f}")
print(f"Mes Más Afectado: {most_affected_month.strftime('%B %Y')} con {most_affected_month_duration:.2f} horas")
print(f"Semana Más Afectada: {most_affected_week} con {most_affected_week_duration:.2f} horas")
print(f"Promedio de Fallas Mensuales: {average_monthly_outages}")
print(f"Promedio de Fallas Semanales: {average_weekly_outages}")
print(f"Falla Acumulativa Más Larga en un Período de 48 horas (en horas): {max_48hr_outage_hours:.2f}")

# Preparar datos para tabulación
monthly_data = []
for month, duration in monthly_duration.items():
    count = monthly_outage_count[month]
    average_duration_per_month = duration / count if count > 0 else 0
    percentage_of_total = (duration / total_duration_hours) * 100
    longest_outage_month = data[data['Timestamp'].dt.to_period('M') == month]['Duration'].max().total_seconds() / 3600
    
    # Calcular días con 1 falla y 2 o más fallas para el mes
    monthly_days = data[data['Timestamp'].dt.to_period('M') == month]
    daily_counts = monthly_days.groupby(monthly_days['Timestamp'].dt.date).size()
    days_with_1_outage = (daily_counts == 1).sum()
    days_with_2_or_more_outages = (daily_counts >= 2).sum()
    
    # Calcular días con fallas de más de 1, 4, y 8 horas
    daily_durations = monthly_days.groupby(monthly_days['Timestamp'].dt.date)['Duration'].sum()
    days_with_outages_over_1h = (daily_durations > timedelta(hours=1)).sum()
    days_with_outages_over_4h = (daily_durations > timedelta(hours=4)).sum()
    days_with_outages_over_8h = (daily_durations > timedelta(hours=8)).sum()
    
    # Calcular días con al menos una falla
    days_with_at_least_one_outage = daily_counts.size

    monthly_data.append([
        month.strftime('%B %Y'),  # Formato mes como "Mes Año"
        count, 
        f"{duration:.2f}", 
        f"{average_duration_per_month:.2f}", 
        f"{longest_outage_month:.2f}",
        days_with_1_outage,
        days_with_2_or_more_outages,
        days_with_outages_over_1h,  # New metric
        days_with_outages_over_4h,  # New metric
        days_with_outages_over_8h,  # New metric
        days_with_at_least_one_outage  # New metric
    ])

# Imprimir tabla de fallas mensuales
print("\nInforme de Fallas Mensuales:")
print(tabulate(monthly_data, headers=[
    "Mes", 
    "Fallas", 
    "Duración (h)", 
    "Duración Promedio ()", 
    "Falla Más Larga (h)",
    "Días con 1 Falla",
    "Días con 2+ Fallas",
    "días con falla>1h",  # New header
    "días con falla>4h",  # New header
    "días con falla>8h",  # New header
    "días por mes >=1 falla"  # New header
], tablefmt="grid"))