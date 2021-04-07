# Generación del lenguaje con GPT2 

GPT-2 se entrenó simplemente para predecir la siguiente palabra en 40 GB de texto de Internet.GPT-2 es un gran modelo de lenguaje basado en transformadores con 1.500 millones de parámetros, entrenado en un conjunto de datos de 8 millones de páginas web. GPT-2 se entrena con un objetivo simple: predecir la siguiente palabra, dadas todas las palabras anteriores dentro de un texto. La diversidad del conjunto de datos hace que este simple objetivo contenga demostraciones naturales de muchas tareas en diversos dominios. GPT-2 es un escalado directo de GPT, con más de 10 veces los parámetros y entrenado en más de 10 veces la cantidad de datos. En tareas de lenguaje como respuesta a preguntas, comprensión de lectura, resumen y traducción, GPT-2 comienza a aprender estas tareas a partir del texto sin procesar, sin utilizar datos de entrenamiento específicos de la tarea. Si bien los puntajes en estas tareas posteriores están lejos de ser el estado de la técnica, sugieren que las tareas pueden beneficiarse de técnicas no supervisadas, con suficientes datos y computación (sin etiquetar).
*[1] Texto obtenido de la web de OpenIA*

## 1 Objetivos 

El objetivo de entrenamiento del modelo de lenguaje se formula como P (salida | entrada). Sin embargo, GPT-2 tenía como objetivo aprender múltiples tareas utilizando el mismo modelo sin supervisión. Para lograrlo, el objetivo de aprendizaje debe modificarse a P (salida | entrada, tarea). Esta modificación se conoce como acondicionamiento de tareas, donde se espera que el modelo produzca diferentes resultados para la misma entrada para diferentes tareas. Por lo tanto, la tarea de acondicionamiento para modelos de lenguaje se lleva a cabo proporcionando ejemplos o instrucciones en lenguaje natural al modelo para realizar una tarea. El acondicionamiento de tareas forma la base para la transferencia de tareas de disparo cero que cubriremos a continuación.

El **Aprendizaje Zero Shot** es un caso especial de transferencia de tareas de disparo cero en el que no se proporcionan ejemplos y el modelo comprende la tarea en función de la instrucción dada. En lugar de reorganizar las secuencias, como se hizo para GPT-1 para el ajuste fino, la entrada a GPT-2 se proporcionó en un formato que esperaba que el modelo entendiera la naturaleza de la tarea y proporcionara respuestas.

*[2] Texto obtenido del articulo El viaje de los modelos Open AI GPT escrito por Priya Sheree*




## Bibliografia
[1] https://openai.com/blog/better-language-models/

[2] https://medium.com/walmartglobaltech/the-journey-of-open-ai-gpt-models-32d95b7b7fb2
