from celery_app import extraer_noticias_infobae, extraer_tn, extraer_ambito

print("enviando orden a RabbitMQ...")
#resultado = extraer_noticias_infobae.delay() #type:ignore
#print(f"Tarea encolada con ID: {resultado.id}")

print("enviando orden de extraer de tn...")
#resultado = extraer_tn.delay() #type:ignore
#print(f"Tarea encolada con ID: {resultado.id}")

print("Enviando orden de extraer de ambito...")
resultado = extraer_ambito.delay()#type:ignore
print(f"Tarea encolada con ID: {resultado.id}")

