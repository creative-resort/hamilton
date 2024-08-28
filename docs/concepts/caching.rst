========
Caching
========

In the context of Hamilton, the term *caching* broadly refers to the ability to "limit redundant computations when executing the same dataflow multiple times". This comes in several flavors that can serve different use cases:

- Faster development feedback loop by skipping redundant computation while authoring your dataflow.
- Checkpoint and resume your dataflow in case of failures
- Tune your dataflow (i.e., similar to hyperparameter tuning in ML) by iterating over inputs and configs

This page first covers how to use caching in Hamilton then details the implementation of the caching mechanisms.

.. warning::

    The caching feature is under active development. If you have questions or encounter any issue, please reach out via GitHub Issues or the Hamilton Slack community server.


Basics
-------

Simply add the ``.with_cache()`` clause to the ``Builder()`` to get started. By default, it will create in the current directory a ``hamilton_cache/`` subdirectory. ``Driver.execute()`` will execute the dataflow as usual, but it will also store metadata and results under ``hamilton_cache/`` for future use. Subsequent calls to ``Driver.execute()`` will leverage the metadata to determine if results can be loaded from disk and skip node execution when possible!


.. code-block:: python

    from hamilton import driver
    import my_dataflow

    dr = (
        driver.Builder()
        .with_module(my_dataflow)
        .with_cache()
        .build()
    )

    dr.execute([...])


Cache to a file
-----------------

By default, caching only uses the Python standard library and relies on the ``pickle`` format to store results, `which comes with caveats <https://grantjenks.com/docs/diskcache/tutorial.html#caveats>`_ and can be impractical. Furthermore, some Python objects can't even be pickled (e.g., text tokenizers with Rust bindings).

Using the Hamilton ``@tag`` decorator, you can cache a node's result to a specific file format (``JSON``, ``CSV``, ``Parquet``, etc.) and circumvent the limitations of ``pickle`` files.


.. code-block:: python

    # my_dataflow.py
    import pandas as pd
    from hamilton.function_modifiers import tag

    def raw_data(path: str) -> pd.DataFrame:
        return pd.read_csv(path)

    @tag(cache="parquet")
    def clean_dataset(raw_data: pd.DataFrame) -> pd.DataFrame:
        raw_data = raw_data.fillna(0)
        return raw_data

    @tag(cache="json")
    def statistics(clean_dataset: pd.DataFrame) -> dict:
        return ...


.. note::

    This aims to match and replace the features of ``hamilton.experimental.h_cache.CachingGraphAdater`` but the API is preliminary.


Resume from checkpoint
-----------------------

The caching feature stores results after each node execution. If execution fails half-way, the ``Driver`` can use the cache metadata and results to resume the run. You can think of it as automatically passing ``.execute(overrides={...})`` from disk.

To resume, you can specify ``.with_cache(resume_from=...)`` which accepts a specific ``run_id`` or the string ``"latest"`` which will automatically get the results from the latest execution, whether it succeeded or failed. The ``run_id`` is automatically generated internally and can be viewed via the cache logger.

.. code-block:: python

    from hamilton import driver
    import my_dataflow

    dr = (
        driver.Builder()
        .with_modules(my_dataflow)
        .with_cache(resume_from="latest")
        .build()
    )

    dr.execute(...)

.. note::

    Using ``resume_from="latest"`` gets the latest run at the moment of building the ``Driver``. Consequently, calling ``Driver.execute()`` multiple times afterwards will always retrieve the same results from that specific point in time.


Control caching behavior
-------------------------

Hamilton's default caching behavior is ideal to get started and typically what you want during iterative development. However, you might want more control over the cache behavior in specific scenario or when moving to production. The caching behavior can be specified at the node-level via either:

- **Dataflow definition** by adding to the function modifiers ``@tag(caching_behavior="ignore")``
- **Driver definition** by passing a list of node name to ``.with_cache(ignore=["foo", "bar"])``

The "Driver definition" approach will always override the behavior specified using ``@tag``. Passing empty lists to ``ignore=[]`` effectively disables all ``@tag``. Querying node names directly from the ``Driver`` is an effective way to select multiple nodes at once:

.. code-block:: python

    import pandas as pd
    from hamilton.driver import Builder
    import my_dataflow

    nodes = Builder().with_modules(my_dataflow).list_available_variables()
    dr = (
        Builder()
        .with_modules(my_dataflow)
        .with_cache(
            ignore=[n.name for n in nodes if "openai" in n.name],
            dont_fingerprint=[n.name for n in nodes if isinstance(n.type, pd.DataFrame)],
        )
        .build()
    )

The next sections illustrate when modifying the caching behavior may be desirable.

Default
~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: auto
   :align: center

   * - Try to retrieve result
     - Store result
     - Fingerprint result
     - Store fingerprint
   * - ✅
     - ✅
     - ✅
     - ✅

First, let's understand the default behavior and assumptions:

1. It is assumed that "the same node (code) with the same data produces the same result", i.e., `idempotence <https://www.astronomer.io/docs/learn/dag-best-practices#dag-design>`_ and non-randomness.
2. All data passing through the dataflow (inputs, overrides, results) is fingerprinted.
3. All result fingerprints are stored in the metadata store.
4. All results are stored in the result store.
5. For each node execution, Hamilton uses the metadata store to determine if it can retrieve results from the result store.

Ignore
~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: auto
   :align: center

   * - Try to retrieve result
     - Store result
     - Fingerprint result
     - Store fingerprint
   * - ❌
     - ❌
     - ❌
     - ❌

You might benefit from caching, but also want to completely disable it for a specific nodes. This is likely the case if it's breaking the 3 defaults settings. In other words, it's non-idempotent, not valuable to store, and hard to uniquely identify. This is typically true for API clients.

Using ``caching_behavior="ignore"`` means the result is never stored nor fingerprinted. The behavior is equivalent to not using ``.with_cache()`` for this node.

.. code-block:: python

    import weather_api

    @tag(caching_behavior="ignore")
    def weather_client(credentials: dict) -> weather_api.Client:
        return weather_api.Client(**credentials)

    def current_temperature(weather_client: weather_api.Client) -> float:
        """Get the current local temperature from a public API"""
        return weather_api.Client(**credentials).get_temperature(...)

    def weather_message(
        current_temperature: float, current_wind: dict, is_rain: bool,
    ) -> str:
        """Interpret the temperature and return a message"""
        if ...:
            return "what a nice day 🌞"
        elif ...:
            return "you should bring your umbrella ☔"
        else:
            return "it's starting to feel cold 🥶"


Always recompute
~~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: auto
   :align: center

   * - Try to retrieve result
     - Store result
     - Fingerprint result
     - Store fingerprint
   * - ❌
     - ✅
     - ✅
     - ✅

If a node is non-idempotent (e.g., read/write with a database) or includes elements of randomness (e.g., training a machine learning model), you might want to ensure the node is computed at each execution.

By specifying ``caching_behavior="always_recompute"``, the node is always computed to fetch the latest temperature. Then, the ``current_temperature`` result is added to the result store and its fingerprint is added to the metadata store as usual.

Consider the following toy example that gets the current temperature from an API and returns a message to the user:

.. code-block:: python

    import weather_api

    def weather_client(credentials: dict) -> weather_api.Client:
        return weather_api.Client(**credentials)

    @tag(caching_behavior="always_recompute")
    def current_temperature(weather_client: weather_api.Client) -> float:
        """Get the current local temperature from a public API"""
        return weather_api.Client(**credentials).get_temperature(...)

    def weather_message(
        current_temperature: float, current_wind: dict, is_rain: bool,
    ) -> str:
        """Interpret the temperature and return a message"""
        if ...:
            return "what a nice day 🌞"
        elif ...:
            return "you should bring your umbrella ☔"
        else:
            return "it's starting to feel cold 🥶"


Don't fingerprint
~~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: auto
   :align: center

   * - Try to retrieve result
     - Store result
     - Fingerprint result
     - Store fingerprint
   * - ✅
     - ✅
     - ❌
     - ❌

Large and complex data objects (e.g., dataframes, machine learning models) can be expensive or unreliable to fingerprint, but may be valuable to cache nonetheless.

In that case, using ``caching_behavior="dont_fingerprint"`` will store the result with a constant fingerprint making the result only retrievable via ``resume_from="latest"``.

For instance, we don't want to store the ``weather_message`` because it's only a set of ``if/else`` that are trivial to compute.

.. code-block:: python

    import weather_api
    from sklearn.ensemble import HistGradientBoostingRegressor

    def weather_client(credentials: dict) -> weather_api.Client:
        return weather_api.Client(**credentials)

    def current_temperature(weather_client: weather_api.Client) -> float:
        """Get the current local temperature from a public API"""
        return weather_api.Client(**credentials).get_temperature(...)

    @tag(caching_behavior="dont_fingerprint")
    def weather_predictor(
        current_temperature: float, current_wind: dict, is_rain: bool,
    ) -> str:
        """Train a model to predict the probability of rain"""
        model = HistGradientBoostingRegressor(...)
        model.train(...)
        return model

.. note::

     An alternative solution is to register a custom fingerprinting function by ``type`` (see **Fingerprinting**).


Don't store result
~~~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: auto
   :align: center

   * - Try to retrieve result
     - Store result
     - Fingerprint result
     - Store fingerprint
   * - ✅
     - ❌
     - ✅
     - ❌

If a node produces results that are large in size, not picklable, or very inexpensive to compute, it might be preferrable to not store it. Also, your environment may have storage or I/O limitations (e.g., web service).


Specifying ``caching_behavior="dont_store_result"`` means Hamilton won't add the result to the result store, but the fingerprint won't be added to the metadata store since it would point to nothing (i.e., "result store miss"). The fingerprint is only used to create the key for children nodes (e.g., ``recommended_outfit``).

For instance, we don't want to store the ``weather_message`` because it's only a set of ``if/else`` that are trivial to compute.

.. code-block:: python

    import llm
    import weather_api

    def weather_client(credentials: dict) -> weather_api.Client:
        return weather_api.Client(**credentials)

    def current_temperature(weather_client: weather_api.Client) -> float:
        """Get the current local temperature from a public API"""
        return weather_api.Client(**credentials).get_temperature(...)

    @tag(caching_behavior="dont_store_result")
    def weather_message(
        current_temperature: float,
        current_wind: dict,
        is_rain: bool,
    ) -> str:
        """Interpret the temperature and return a message"""
        if ...:
            return "what a nice day 🌞"
        elif ...:
            return "you should bring your umbrella ☔"
        else:
            return "it's starting to feel cold 🥶"

    def recommend_outfit(ll_client: llm.Client, weather_message: str) -> str:
        """Use an LLM to generate an outfit suggestion based on the weather"""
        prompt = f"Make an outfit suggestion based on: {weather_message}""
        response = llm.Client.generate(prompt)
        return response


.. note::

    Even when ``caching_behavior="dont_store_result"``, the adapter will try to retrieve stored values that could have been produce before this behavior was set.


Summary
~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: auto
   :align: center

   * - Behavior
     - Try to retrieve result
     - Store result
     - Fingerprint result
     - Store fingerprint
   * - Default
     - ✅
     - ✅
     - ✅
     - ✅
   * - Ignore
     - ❌
     - ❌
     - ❌
     - ❌
   * - Always recompute
     - ❌
     - ✅
     - ✅
     - ✅
   * - Don't fingerprint
     - ✅
     - ✅
     - ❌
     - ❌
   * - Don't store result
     - ✅
     - ❌
     - ✅
     - ❌

Inspect cache logic
----------------------

You can monitor and log the cache behavior by retrieving the module's logger and set the logging level to ``INFO`` or ``DEBUG``. Then, ``Driver.execute()`` will log events such as "cache hit", "cache miss", and "retrieval errors".

.. code-block:: python

    import logging

    logger = logging.getLogger("hamilton.caching.adapters")
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())


Storage
---------

It is possible to directly interact with the metadata and results via the


Fingerprinting
----------------

For caching to be possible, Hamilton needs to uniquely identify data. Hamilton supports all Python primitive types (``int``, ``str``, ``dict``, etc.) by default and popular libraries via extensions (e.g., ``pandas``). If an object type isn't supported, Hamilton will fingerprint the object's internal ``__dict__`` attribute or return a constant hash if it fails.

You can add support for new types via the ``hamilton.caching.fingerprinting`` module. It uses `@functools.singledispatch <https://docs.python.org/3/library/functools.html#functools.singledispatch>`_ to register the hashing function per Python type. The function must return a ``str``.

.. code-block:: python

    from hamilton.caching import fingerprinting

    class MyComplexObject:
        ...

    @fingerprinting.hash_value.register(MyComplexObject)
    def hash_my_custom_type(obj) -> str
        # ...
        deterministic_unique_id = "..."
        return deterministic_unique_id


Technical implementation
------------------------

Glossary
~~~~~~~~

In simple terms, an effective caching feature should guarantee "for the same inputs and code, if this was previously computed, read the stored values instead of recomputing". In practice, this requires multiple decoupled parts:

- **Result store**: key-value mapping between a ``context_key`` and the result. It doesn't anything else about caching. The choice of storage affects latency, parallelism, fault tolerance, etc.
- **Metadata store**: store node execution metadata including: node name, code version, inputs data version, output data version, execution history. This information allows to recreate the ``context_key`` to query the result store.
- **Caching behavior**: algorithm that interacts with the metadata store to decide whether to compute a node or try to read/write values with the result store.
- **Execution context**: When executing a single node, it uses a specific code version and specific input values. NOTE. the node name is irrelevant to the execution context. This is important for parameterize where multiple parameterization might lead to the same results but different node names.
- **Fingerprinting function**: the function to determine a fingerprint (default is recursive primitive hashing). An example alternative fingerprinting strategy could be to hash the index of a dataframe rather than hash its row content (user is responsible to guarantee unique ids for each row)


Algorithm axioms
~~~~~~~~~~~~~~~~

Hamilton dataflows are directed acyclic graphs (DAGs). When executing a dataflow, two components are at play: the **code** that defines the transformations, and the **data** that flows through the DAG.

Having a visual representation can help understand the caching behavior. For the purpose of this page, **nodes represent code** and **edges represent data**. Intuitively, we say the picture shows "data flows into the code, and transformed data flows out of it".

.. note::
    This is slightly different from the usual Hamilton dataflow visualizations.

Before building an algorithm, it's useful to state axioms that it should adhere to. Writing down these statements and deriving logical conclusions also helps us determine edge cases.

1. A Hamilton node is consituted of **code**, ``>=0`` **input data**, and ``==1`` **output data** (the value ``None`` is considered data).
2. The **output data** of a Hamilton node can be the **input data** of ``>=0`` Hamilton nodes.
    a. If **output data**
3. **Code** can be hashed (i.e., derive a deterministic identifier from it)
4. **Data** can be hashed
5. **Code** and **data** use different hashing functions
6. **Code** doesn't depend on **data** (i.e., the DAG of transformations exists without data)
    a. **Code** can be hashed before execution
7. **External data** (i.e., ``inputs`` and ``overrides`` passed to ``Driver.execute()``) doesn't depend on **code**
    a. **External data** can be hashed before execution
8. **Output data**  (i.e., result of a transformation) depends on **code** and **input data**.
9. The same **code** and **input data** produces the same **output data** (idempotence).
    a. On repeat (**code**, **input data**), we can skip computation and read from cache
    b. Other mechanisms are required when this assumption is broken.
10. If **code** changes or **input data** changes, we don't know what the **output data** will be.
    a. We must recompute the transformation with the new **code** and **input data**.
11. Given ``6.``, if **code** changes, we know we must recompute without having to verify if **input data** changed.
    a. if **code** is the same, we must verify **input data** is the same to know if **output data** can be read from cache.


Others
-------

Caching has many business benefits:

- reduced computation cost: don't redo previous computation
- reduced storage cost: don't store redundant computation results
- reduced development time: help you author new dataflows, but also debug failures
- fine-grained lineage: a side-effect of caching is that you can track what data is read / written by dataflows
